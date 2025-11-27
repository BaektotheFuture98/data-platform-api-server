import json, re, os

def make_file(path: str, content: json) -> None: 
    with open(path, "w", encoding="utf-8") as f: 
        f.write(content)
        # json.dump(content, f, ensure_ascii=False, indent=4)

def update_service_in_bash(script_path: str, service_name: str) -> None:
    """
    Bash 스크립트 내의 services=(...) 부분을 단일 서비스로 교체합니다.
    Args:
        script_path (str): 수정할 bash 파일 경로
        service_name (str): 새롭게 넣을 서비스 이름
    """
    try:
        # 파일 내용 읽기
        with open(script_path, "r", encoding="utf-8") as f:
            content = f.read()

        # 새로운 services 라인 생성
        new_line = f'services=("{service_name}")'

        # 정규식으로 기존 services=(...) 라인 교체
        updated_content = re.sub(r'services=\([^)]+\)', new_line, content)

        # 수정된 내용 덮어쓰기
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(updated_content)

        print(f"✅ {script_path} 내 services 값이 '{service_name}' 으로 변경되었습니다.")

    except FileNotFoundError:
        print(f"❌ 파일을 찾을 수 없습니다: {script_path}")
    except Exception as e:
        print(f"⚠️ 업데이트 중 오류 발생: {e}")

def get_unique_project_name(base_path: str, project_name: str, counter: int = 1, pad_width: int = 3) -> int:
    """
    이미 존재하는 project_name이면 -001, -002 ... 붙이기 위한 counter를 재귀적으로 반환.
    counter = 1 → project_name 그대로 사용
    counter = 2 → project_name-001
    counter = 3 → project_name-002
    """
    # 존재하는 항목 목록 가져오기
    existing = set(os.listdir(base_path)) if os.path.isdir(base_path) else set()
    print(f"counter : {counter} \n existing : {existing}")
    # counter가 1이면 원래 이름 검사, 2 이상이면 -001 형태로 검사
    if counter == 1:
        unique_name = project_name
    else:
        unique_name = f"{project_name}_{str(counter).zfill(pad_width)}"

    # 이미 존재한다면 counter를 증가시키고 재귀 호출
    if unique_name in existing:
        print(f"존재함 => project_name : {project_name}, counter : {counter}")
        return get_unique_project_name(base_path, project_name, counter + 1, pad_width)
    else:
        print(f"존재 하지 않음 => project_name : {project_name}, counter : {counter}")
        return counter