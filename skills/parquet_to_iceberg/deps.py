import re
from pathlib import Path

PYICEBERG_REQ = 'pyiceberg[sql-sqlite]>=0.7.0'

ICEBERG_GROUP = 'org.apache.iceberg'
ICEBERG_ARTIFACT = 'iceberg-spark-runtime-3.5_2.12'
ICEBERG_ARTIFACT_SBT = 'iceberg-spark-runtime-3.5'  # sbt %% appends _2.12 automatically
ICEBERG_VERSION = '1.5.0'


def update_dependencies(project_root: Path) -> list[Path]:
    """Update build files in the project.

    Python build files (requirements.txt, pyproject.toml) are only looked up
    at the project root. JVM build files (pom.xml, build.gradle[.kts],
    build.sbt) are scanned recursively to support nested module layouts.

    Returns the list of files that were actually modified.
    """
    updated: list[Path] = []

    req_file = project_root / "requirements.txt"
    if req_file.exists() and _update_requirements_txt(req_file):
        updated.append(req_file)

    pyproject = project_root / "pyproject.toml"
    if pyproject.exists() and _update_pyproject_toml(pyproject):
        updated.append(pyproject)

    for build_file in sorted(project_root.rglob("pom.xml")):
        if _update_pom_xml(build_file):
            updated.append(build_file)
    for name in ("build.gradle", "build.gradle.kts"):
        for build_file in sorted(project_root.rglob(name)):
            if _update_build_gradle(build_file):
                updated.append(build_file)
    for build_file in sorted(project_root.rglob("build.sbt")):
        if _update_build_sbt(build_file):
            updated.append(build_file)

    return updated


def _update_requirements_txt(path: Path) -> bool:
    content = path.read_text()
    if "pyiceberg" in content:
        return False
    path.write_text(content.rstrip() + f"\n{PYICEBERG_REQ}\n")
    return True


def _update_pyproject_toml(path: Path) -> bool:
    content = path.read_text()
    if "pyiceberg" in content:
        return False
    new_content = re.sub(
        r'(dependencies\s*=\s*\[)(.*?)(\])',
        lambda m: m.group(1) + m.group(2).rstrip() + f'\n  "{PYICEBERG_REQ}",\n' + m.group(3),
        content,
        flags=re.DOTALL,
    )
    if new_content == content:
        return False
    path.write_text(new_content)
    return True


def _update_pom_xml(path: Path) -> bool:
    content = path.read_text()
    if ICEBERG_ARTIFACT in content:
        return False
    iceberg_dep = (
        f'        <dependency>\n'
        f'            <groupId>{ICEBERG_GROUP}</groupId>\n'
        f'            <artifactId>{ICEBERG_ARTIFACT}</artifactId>\n'
        f'            <version>{ICEBERG_VERSION}</version>\n'
        f'        </dependency>\n'
    )
    if '</dependencies>' in content:
        new_content = content.replace('</dependencies>', iceberg_dep + '    </dependencies>', 1)
    else:
        block = f'    <dependencies>\n{iceberg_dep}    </dependencies>\n'
        new_content = content.replace('</project>', block + '</project>', 1)
        if new_content == content:
            return False
    path.write_text(new_content)
    return True


def _update_build_gradle(path: Path) -> bool:
    content = path.read_text()
    if ICEBERG_ARTIFACT in content:
        return False
    coordinate = f"'{ICEBERG_GROUP}:{ICEBERG_ARTIFACT}:{ICEBERG_VERSION}'"
    line = f"    implementation {coordinate}\n"
    if re.search(r'dependencies\s*\{', content):
        new_content = re.sub(
            r'(dependencies\s*\{)',
            lambda m: m.group(1) + "\n" + line,
            content,
            count=1,
        )
    else:
        new_content = content.rstrip() + f"\n\ndependencies {{\n{line}}}\n"
    path.write_text(new_content)
    return True


def _update_build_sbt(path: Path) -> bool:
    content = path.read_text()
    if "iceberg-spark-runtime" in content:
        return False
    dep_line = (
        f'libraryDependencies += "{ICEBERG_GROUP}" %% "{ICEBERG_ARTIFACT_SBT}" % "{ICEBERG_VERSION}"\n'
    )
    path.write_text(content.rstrip() + "\n\n" + dep_line)
    return True
