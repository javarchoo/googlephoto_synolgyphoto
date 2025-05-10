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
import glob

# 설정
ROOT_DIR = Path(r"G:\Download\Takeout\work\src")       # 원본 폴더
DEST_DIR = Path(r"G:\Download\Takeout\work\dest")  # 이동 대상 폴더
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
    except Exception:
        pass
    return None

# JSON 메타데이터에서 날짜 및 위치 추출
def get_json_taken_date_and_location(filepath):
    pattern = str(filepath) + ".*.json"
    matches = glob.glob(pattern)
    preferred_keys = [
        "photoTakenTime", "creationTime", "mediaCreateTime", "trackCreateTime",
        "takenTimestamp", "dateAcquired", "modificationTime"
    ]
    for json_file in matches:
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                date_obj = None
                for key in preferred_keys:
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
        except Exception:
            continue
    return None, None

# sub function: datetime 변환
def extract_datetime(value):
    """날짜 문자열을 datetime 객체로 변환"""
    try:
        if "." in value:
            value = value.split(".")[0]
        return datetime.strptime(value.strip(), "%Y:%m:%d %H:%M:%S")
    except:
        return None

# sub function: GPSDateStamp + GPSTimeStamp 조합
def extract_gps_datetime(exif):
    """GPSDateStamp + GPSTimeStamp 조합"""
    gps_date = exif.get("GPS Date Stamp")
    gps_time = exif.get("GPS Time Stamp")
    if gps_date and gps_time:
        try:
            h, m, s = map(float, re.findall(r"\d+\.?\d*", gps_time))
            time_str = f"{int(h):02}:{int(m):02}:{int(s):02}"
            dt_str = f"{gps_date} {time_str}"
            return datetime.strptime(dt_str, "%Y:%m:%d %H:%M:%S")
        except:
            return None
    return None

# EXIFTool을 사용하여 촬영 시간 추정
def get_best_taken_datetime(filepath):
    try:
        result = subprocess.run(
            ["exiftool", 
             "-DateTimeOriginal", 
             "-CreateDate", 
             "-SubSecDateTimeOriginal",
             "-DateTimeDigitized",
             "-ModifyDate",
             "-GPSDateStamp",
             "-GPSTimeStamp",
             filepath],
            capture_output=True, text=True, check=True
        )
        
        exif = {}
        for line in result.stdout.splitlines():
            if ":" in line:
                key, val = line.split(":", 1)
                exif[key.strip()] = val.strip()

        source_priority = [
            ("Date/Time Original", "DateTimeOriginal"),
            ("Create Date", "CreateDate"),
            ("Sub Sec Date/Time Original", "SubSecDateTimeOriginal"),
            ("Date/Time Digitized", "DateTimeDigitized"),
            ("Modify Date", "ModifyDate")
        ]
        
        for tag_name, source_name in source_priority:
            if tag_name in exif:
                dt = extract_datetime(exif[tag_name])
                if dt:
                    return dt, source_name

        gps_dt = extract_gps_datetime(exif)
        if gps_dt:
            return gps_dt, "GPSDateStamp+GPSTimeStamp"
        
    except Exception as e:
        print(f"[⚠️ EXIFTool 시간 추정 실패] {filepath} → {e}")
        return None, None

# 파일 이름에서 날짜 추출
def parse_date_from_filename(name):
    def is_valid_date(dt):
        return datetime(2005, 1, 1) <= dt <= datetime(2025, 12, 31)

    # 1차: 4자리 연도
    patterns_4digit = [
        r"(\d{4})[.\-_]?(\d{2})[.\-_]?(\d{2})",
        r"(\d{4})[.\-_]?(\d{1,2})[.\-_]?(\d{1,2})"
    ]
    for pattern in patterns_4digit:
        match = re.search(pattern, name)
        if match:
            try:
                y, m, d = match.groups()
                dt = datetime(int(y), int(m.zfill(2)), int(d.zfill(2)))
                if is_valid_date(dt):
                    return dt
            except:
                continue

    # 2차: 2자리 연도 → 20xx로 간주
    patterns_2digit = [
        r"(\d{2})[.\-_]?(\d{2})[.\-_]?(\d{2})",
        r"(\d{2})[.\-_]?(\d{1,2})[.\-_]?(\d{1,2})"
    ]
    for pattern in patterns_2digit:
        match = re.search(pattern, name)
        if match:
            try:
                y, m, d = match.groups()
                full_year = 2000 + int(y)  # 예: "18" → 2018
                dt = datetime(full_year, int(m.zfill(2)), int(d.zfill(2)))
                if is_valid_date(dt):
                    return dt
            except:
                continue

    # fallback
    return datetime(2005, 1, 1)

# EXIF 위치 정보 추가
def append_gps_tags(cmd, lat, lon):
    cmd.append(f"-GPSLatitude={lat}")
    cmd.append(f"-GPSLatitudeRef={'N' if lat >= 0 else 'S'}")
    cmd.append(f"-GPSLongitude={lon}")
    cmd.append(f"-GPSLongitudeRef={'E' if lon >= 0 else 'W'}")

def update_exiftool_taken_date(filepath, date_obj, location=None):
    date_str = date_obj.strftime("%Y:%m:%d %H:%M:%S")
    suffix = filepath.suffix.lower()
    if suffix in {".webp"}:
        print(f"[⚠️ EXIF 미지원 포맷] {filepath} - 건너뜀")
        return

    lat, lon = location if location else (None, None)
    cmd = ["exiftool", f"-AllDates={date_str}"]
    if suffix in {".mp4", ".mov", ".3gp"}:
        cmd += [f"-MediaCreateDate={date_str}", f"-TrackCreateDate={date_str}"]
    if location:
        append_gps_tags(cmd, lat, lon)
    cmd += ["-overwrite_original", str(filepath)]

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        return
    except subprocess.CalledProcessError:
        pass

    # Exift정보 꼬여서 업데이트 안될 때 파일을 다시 쓰고 업데이트 시도
    try:
        img = Image.open(filepath)
        img.save(filepath)
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        # print(f"[⚠️ 재저장 후 AllDates + 위치 성공] {filepath}")
        return
    except Exception as e:
        print(f"[⚠️ 재저장 후 AllDates + 위치 실패] {filepath} → {e}")

    fallback_cmd = ["exiftool", f"-DateTimeOriginal={date_str}"]
    if location:
        append_gps_tags(fallback_cmd, lat, lon)
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
            date_taken, source_name = get_best_taken_datetime(file)
            #method = "EXIFTool"
            method = "FILENAME"

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
            log(dest_path, success_list)
        else:
            dest_dir = UNDEFINED_DIR
            dest_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(file, dest_dir / file.name)
            log(dest_dir / file.name, fail_list)
    except Exception as e:
        print(f"[⚠️ 처리 실패] {file} → {e}")
        log(file, fail_list)

# 메인 실행
def run_parallel_processing():
    all_files = [p for p in ROOT_DIR.rglob("*") if p.suffix.lower() in EXTENSIONS]
    num_workers = max(1, cpu_count() * 9 // 10)

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
                    print(f"   처리 완료: {completed}/{len(all_files)}", end="\r")

        with open(LOG_SUCCESS, "w", encoding="utf-8") as f:
            for path in success_list:
                f.write(f"{path}\n")

        with open(LOG_FAIL, "w", encoding="utf-8") as f:
            for path in fail_list:
                f.write(f"{path}\n")

if __name__ == "__main__":
    run_parallel_processing()
