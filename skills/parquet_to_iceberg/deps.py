import re
from pathlib import Path

PYICEBERG_REQ = 'pyiceberg[sql-sqlite]>=0.7.0'

ICEBERG_GROUP = 'org.apache.iceberg'
ICEBERG_ARTIFACT = 'iceberg-spark-runtime-3.5_2.12'
ICEBERG_VERSION = '1.5.0'


def update_dependencies(project_root: Path) -> None:
    req_file = project_root / "requirements.txt"
    pyproject = project_root / "pyproject.toml"
    pom = project_root / "pom.xml"
    gradle = project_root / "build.gradle"
    gradle_kts = project_root / "build.gradle.kts"

    if req_file.exists():
        _update_requirements_txt(req_file)
    if pyproject.exists():
        _update_pyproject_toml(pyproject)
    if pom.exists():
        _update_pom_xml(pom)
    if gradle.exists():
        _update_build_gradle(gradle)
    if gradle_kts.exists():
        _update_build_gradle(gradle_kts)


def _update_requirements_txt(path: Path) -> None:
    content = path.read_text()
    if "pyiceberg" in content:
        return
    path.write_text(content.rstrip() + f"\n{PYICEBERG_REQ}\n")


def _update_pyproject_toml(path: Path) -> None:
    content = path.read_text()
    if "pyiceberg" in content:
        return
    content = re.sub(
        r'(dependencies\s*=\s*\[)(.*?)(\])',
        lambda m: m.group(1) + m.group(2).rstrip() + f'\n  "{PYICEBERG_REQ}",\n' + m.group(3),
        content,
        flags=re.DOTALL,
    )
    path.write_text(content)


def _update_pom_xml(path: Path) -> None:
    content = path.read_text()
    if ICEBERG_ARTIFACT in content:
        return
    iceberg_dep = (
        f'        <dependency>\n'
        f'            <groupId>{ICEBERG_GROUP}</groupId>\n'
        f'            <artifactId>{ICEBERG_ARTIFACT}</artifactId>\n'
        f'            <version>{ICEBERG_VERSION}</version>\n'
        f'        </dependency>\n'
    )
    if '</dependencies>' in content:
        content = content.replace('</dependencies>', iceberg_dep + '    </dependencies>', 1)
    else:
        block = f'    <dependencies>\n{iceberg_dep}    </dependencies>\n'
        content = content.replace('</project>', block + '</project>', 1)
    path.write_text(content)


def _update_build_gradle(path: Path) -> None:
    content = path.read_text()
    if ICEBERG_ARTIFACT in content:
        return
    coordinate = f"'{ICEBERG_GROUP}:{ICEBERG_ARTIFACT}:{ICEBERG_VERSION}'"
    line = f"    implementation {coordinate}\n"
    if re.search(r'dependencies\s*\{', content):
        content = re.sub(
            r'(dependencies\s*\{)',
            lambda m: m.group(1) + "\n" + line,
            content,
            count=1,
        )
    else:
        content = content.rstrip() + f"\n\ndependencies {{\n{line}}}\n"
    path.write_text(content)
