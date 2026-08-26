from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parents[2]


def test_api_extra_pins_compatible_versions() -> None:
    pyproject = tomllib.loads(
        (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )

    api_dependencies = pyproject["project"]["optional-dependencies"]["api"]

    assert api_dependencies == [
        "fastapi==0.141.1",
        "uvicorn[standard]==0.52.3",
        "pydantic==2.13.4",
        "httpx==0.28.1",
    ]
