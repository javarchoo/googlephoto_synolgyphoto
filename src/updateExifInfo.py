#!/usr/bin/env python
# coding: utf-8
# Google Photo Synology Photo EXIF 정보 업데이트

from pathlib import Path
from PIL import Image
import json
import re
import shutil
import subprocess
from datetime import datetime
from tqdm import tqdm
from multiprocessing import Pool, cpu_count, Manager
import time
import glob

# 설정
ROOT_DIR = Path(r"G:\Download\Takeout\work2\src")       # 원본 폴더
DEST_DIR = Path(r"G:\Download\Takeout\work2\dest")  # 이동 대상 폴더
CHECK_DIR = DEST_DIR / "check"
UNDEFINED_DIR = DEST_DIR / "undefined"
LOG_SUCCESS = ROOT_DIR / "success_log.txt"
LOG_FAIL = ROOT_DIR / "fail_log.txt"

EXTENSIONS = {".3gp", ".dng", ".gif", ".heic", ".jpeg", ".jpg", ".mov", ".mp4", ".png", ".webp"}

# 로그 기록
def log(path, log_list):
    log_list.append(str(path))

# EXIF에서 촬영일 추출
def get_exif_taken_date(filepath):
    try:
        img = Image.open(filepath)
        exif_data = img._getexif()
        if exif_data and 36867 in exif_data:
            return datetime.strptime(exif_data[36867], "%Y:%m:%d %H:%M:%S")
    except:
        pass
    return None

# JSON 메타데이터에서 날짜 및 위치 추출
def get_json_taken_date_and_location(filepath):
    pattern = str(filepath) + ".*.json"
    matches = glob.glob(pattern)
    for json_file in matches:
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                date_obj = None
                for key in ["photoTakenTime", "creationTime", "mediaCreateTime", "trackCreateTime"]:
                    if key in data:
                        timestamp = int(data[key]["timestamp"])
                        date_obj = datetime.utcfromtimestamp(timestamp)
                        break
                location = None
                for geo_key in ["geoData", "geoDataExif"]:
                    if geo_key in data:
                        geo = data[geo_key]
                        lat, lon = geo.get("latitude", 0), geo.get("longitude", 0)
                        if lat != 0.0 or lon != 0.0:
                            location = (lat, lon)
                            break
                if date_obj:
                    return date_obj, location
        except:
            continue
    return None, None

# 파일 이름에서 날짜 추출
def parse_date_from_filename(name):
    patterns = [
        r"(\d{4})[.\-_]?(\d{2})[.\-_]?(\d{2})",
        r"(\d{4})[.\-_]?(\d{1,2})[.\-_]?(\d{1,2})"
    ]
    for pattern in patterns:
        match = re.search(pattern, name)
        if match:
            try:
                y, m, d = match.groups()
                return datetime(int(y), int(m.zfill(2)), int(d.zfill(2)))
            except:
                continue
    return None

# EXIF 날짜 및 위치 업데이트
def update_exiftool_taken_date(filepath, date_obj, location=None):
    date_str = date_obj.strftime("%Y:%m:%d %H:%M:%S")
    suffix = filepath.suffix.lower()
    if suffix in {".webp"}:
        print(f"[⚠️ EXIF 미지원 포맷] {filepath} - 건너뜀")
        return

    lat, lon = None, None
    cmd = []
    if suffix in {".mp4", ".mov", ".3gp"}:
        cmd = [
            "exiftool",
            f"-AllDates={date_str}",
            f"-MediaCreateDate={date_str}",
            f"-TrackCreateDate={date_str}"
        ]
    else:
        cmd = ["exiftool", f"-AllDates={date_str}"]

    if location:
        lat, lon = location
        cmd.append(f"-GPSLatitude={lat}")
        cmd.append(f"-GPSLatitudeRef={'N' if lat >= 0 else 'S'}")
        cmd.append(f"-GPSLongitude={lon}")
        cmd.append(f"-GPSLongitudeRef={'E' if lon >= 0 else 'W'}")

    cmd += ["-overwrite_original", str(filepath)]

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError:

        # Exift정보 꼬여서 업데이트 안될 때 파일을 다시 쓰고 업데이트 시도도
        try:
            img = Image.open(filepath)
            img.save(filepath)
            png_cmd = ["exiftool", f"-AllDates={date_str}"]
            if location:
                png_cmd.append(f"-GPSLatitude={lat}")
                png_cmd.append(f"-GPSLatitudeRef={'N' if lat >= 0 else 'S'}")
                png_cmd.append(f"-GPSLongitude={lon}")
                png_cmd.append(f"-GPSLongitudeRef={'E' if lon >= 0 else 'W'}")
            png_cmd += ["-overwrite_original", str(filepath)]
            subprocess.run(png_cmd, check=True, capture_output=True, text=True)
            print(f"[⚠️ 파일 재저장 후 AllDates + 위치 성공] {filepath}")
            return
        except Exception as png_err:
            print(f"[❌ 파일 재저장 후 업데이트 실패] {filepath} → {png_err}")

        # fallback with DateTimeOriginal
        fallback_cmd = ["exiftool", f"-DateTimeOriginal={date_str}"]
        if location:
            fallback_cmd.append(f"-GPSLatitude={lat}")
            fallback_cmd.append(f"-GPSLatitudeRef={'N' if lat >= 0 else 'S'}")
            fallback_cmd.append(f"-GPSLongitude={lon}")
            fallback_cmd.append(f"-GPSLongitudeRef={'E' if lon >= 0 else 'W'}")
        fallback_cmd += ["-overwrite_original", str(filepath)]
        try:
            subprocess.run(fallback_cmd, check=True, capture_output=True, text=True)
            print(f"[⚠️ Fallback 성공] {filepath} - DateTimeOriginal 및 위치 설정")
        except subprocess.CalledProcessError as e2:
            print(f"[❌ ExifTool 최종 실패] {filepath}\n→ {e2.stderr.strip()}")

# 파일 처리 함수
def process_file_worker(file, queue, success_list, fail_list):
    try:
        process_file((file, success_list, fail_list))
    finally:
        queue.put(1)

def process_file(args):
    file, success_list, fail_list = args
    try:
        date_taken = get_exif_taken_date(file)
        location = None
        method = "EXIF"

        if not date_taken:
            date_taken, location = get_json_taken_date_and_location(file)
            method = "JSON"

        if not date_taken:
            date_taken = parse_date_from_filename(file.name)
            method = "FILENAME"

        if date_taken:
            year, month = str(date_taken.year), f"{date_taken.month:02}"
            dest_dir = (CHECK_DIR if method == "FILENAME" else DEST_DIR) / year / month
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_path = dest_dir / file.name
            shutil.move(file, dest_path)
            update_exiftool_taken_date(dest_path, date_taken, location)
            log(file, success_list)
        else:
            dest_dir = UNDEFINED_DIR
            dest_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(file, dest_dir / file.name)
            log(file, fail_list)
    except:
        log(file, fail_list)

# 메인 실행
def run_parallel_processing():
    all_files = [p for p in ROOT_DIR.rglob("*") if p.suffix.lower() in EXTENSIONS]
    num_workers = max(1, cpu_count() * 3 // 4)

    with Manager() as manager:
        queue = manager.Queue()
        success_list = manager.list()
        fail_list = manager.list()

        with Pool(processes=num_workers) as pool:
            for file in all_files:
                pool.apply_async(process_file_worker, args=(file, queue, success_list, fail_list))

            with tqdm(total=len(all_files), desc="📁 정리 중") as pbar:
                completed = 0
                while completed < len(all_files):
                    queue.get()
                    completed += 1
                    pbar.update(1)
                    print(f"   처리 완료: {completed}/{len(all_files)}", end='\r')

        with open(LOG_SUCCESS, "w", encoding="utf-8") as f:
            for path in success_list:
                f.write(f"{path}\n")

        with open(LOG_FAIL, "w", encoding="utf-8") as f:
            for path in fail_list:
                f.write(f"{path}\n")

if __name__ == "__main__":
    run_parallel_processing()
