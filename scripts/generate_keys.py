"""
scripts/generate_keys.py
Otomatis generate Fernet key dan Secret key untuk Airflow,
lalu tulis ke file .env di root project.
"""
import secrets
import base64
import os
from pathlib import Path

def generate_fernet_key() -> str:
    """Generate Fernet key yang valid untuk Airflow enkripsi."""
    key = base64.urlsafe_b64encode(secrets.token_bytes(32))
    return key.decode()

def generate_secret_key() -> str:
    """Generate random secret key untuk Airflow webserver."""
    return secrets.token_hex(32)

def update_env_file(env_path: Path, fernet_key: str, secret_key: str):
    """Tulis keys ke file .env."""
    if not env_path.exists():
        print(f"ERROR: File {env_path} tidak ditemukan!")
        print("Pastikan kamu sudah copy .env.example menjadi .env terlebih dahulu.")
        print("Caranya: copy .env.example .env  (Windows)")
        return False

    content = env_path.read_text()

    # Replace baris AIRFLOW_FERNET_KEY
    lines = content.splitlines()
    new_lines = []
    for line in lines:
        if line.startswith("AIRFLOW_FERNET_KEY="):
            new_lines.append(f"AIRFLOW_FERNET_KEY={fernet_key}")
        elif line.startswith("AIRFLOW_SECRET_KEY="):
            new_lines.append(f"AIRFLOW_SECRET_KEY={secret_key}")
        else:
            new_lines.append(line)

    env_path.write_text("\n".join(new_lines) + "\n")
    return True

def main():
    # Cari file .env di root project (satu folder di atas scripts/)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    env_path = project_root / ".env"

    print("=" * 50)
    print("  Airflow Key Generator")
    print("=" * 50)

    fernet_key = generate_fernet_key()
    secret_key = generate_secret_key()

    print(f"\nFernet Key : {fernet_key[:20]}...")
    print(f"Secret Key : {secret_key[:20]}...")

    if update_env_file(env_path, fernet_key, secret_key):
        print(f"\n✅ Keys berhasil ditulis ke {env_path}")
        print("\nLangkah selanjutnya:")
        print("  docker compose up airflow-init")
    else:
        print("\nKeys yang di-generate (copy manual ke .env kamu):")
        print(f"  AIRFLOW_FERNET_KEY={fernet_key}")
        print(f"  AIRFLOW_SECRET_KEY={secret_key}")

if __name__ == "__main__":
    main()