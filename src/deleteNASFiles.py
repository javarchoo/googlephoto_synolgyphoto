import os
from pathlib import Path
from tqdm import tqdm

def delete_matching_files(original_dir, target_dir, log_path="deleted_files.log", dry_run=True):
    original_dir = Path(original_dir)
    target_dir = Path(target_dir)
    deleted_files = []

    # original 내의 모든 파일 목록 수집
    all_files = [f for f in original_dir.rglob("*") if f.is_file()]
    candidates = []

    with tqdm(total=len(all_files), desc="📂 파일 검사 중") as pbar:
        for idx, orig_file in enumerate(all_files, start=1):
            relative_path = orig_file.relative_to(original_dir)
            target_file = target_dir / relative_path
            if target_file.exists():
                candidates.append(target_file)
                tqdm.write(f"[{idx}/{len(all_files)}] 🔍 삭제 후보: {target_file}")
            pbar.update(1)

    if dry_run:
        print(f"\n🔎 총 {len(candidates)}개의 파일이 삭제 대상입니다.")
        print(f"📝 확인용 로그 저장: {log_path}")
        with open(log_path, "w", encoding="utf-8") as f:
            for path in candidates:
                f.write(f"{path}\n")
        print("⚠️ dry_run=True 설정으로 실제 삭제는 하지 않았습니다.")
    else:
        print(f"\n🚨 삭제 실행 중... 총 {len(candidates)}개 파일 삭제 예정")
        for idx, file in enumerate(candidates, start=1):
            try:
                file.unlink()
                deleted_files.append(str(file))
                tqdm.write(f"[{idx}/{len(candidates)}] 🗑️ 삭제됨: {file}")
            except Exception as e:
                tqdm.write(f"[{idx}/{len(candidates)}] ⚠️ 삭제 실패: {file} - {e}")

        if deleted_files:
            with open(log_path, "w", encoding="utf-8") as f:
                for path in deleted_files:
                    f.write(f"{path}\n")
            print(f"📝 삭제된 파일 로그 저장 완료: {log_path}")
        else:
            print("✅ 삭제된 파일이 없습니다.")

# 사용 예시
# Step 1: 삭제 미리보기 (Dry Run)
# delete_matching_files("G:/Download/original", "Z:/NAS/target", dry_run=True)

# Step 2: 확인 후 삭제
# delete_matching_files("G:/Download/original", "Z:/NAS/target", dry_run=False)

if __name__ == "__main__":
    delete_matching_files("G:\Download\Takeout\work_undefined\dest\check", "Z:", dry_run=False)