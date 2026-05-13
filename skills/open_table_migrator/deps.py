import difflib
import re
from dataclasses import dataclass
from pathlib import Path


PYICEBERG_REQ = 'pyiceberg[sql-sqlite]>=0.7.0'

ICEBERG_GROUP = 'org.apache.iceberg'
ICEBERG_ARTIFACT = 'iceberg-spark-runtime-3.5_2.12'
ICEBERG_ARTIFACT_SBT = 'iceberg-spark-runtime-3.5'  # sbt %% appends _2.12 automatically
ICEBERG_VERSION = '1.5.0'


@dataclass(frozen=True)
class BuildFileUpdate:
    file: Path
    original: str
    modified: str

    @property
    def diff(self) -> str:
        return "".join(difflib.unified_diff(
            self.original.splitlines(keepends=True),
            self.modified.splitlines(keepends=True),
            fromfile=str(self.file),
            tofile=str(self.file),
        ))


def plan_dependencies_update(project_root: Path) -> list[BuildFileUpdate]:
    """Compute build-file changes. No filesystem writes."""
    plans: list[BuildFileUpdate] = []

    req_file = project_root / "requirements.txt"
    if req_file.exists():
        original = req_file.read_text()
        modified = _plan_requirements_txt(original)
        if modified is not None and modified != original:
            plans.append(BuildFileUpdate(file=req_file, original=original, modified=modified))

    pyproject = project_root / "pyproject.toml"
    if pyproject.exists():
        original = pyproject.read_text()
        modified = _plan_pyproject_toml(original)
        if modified is not None and modified != original:
            plans.append(BuildFileUpdate(file=pyproject, original=original, modified=modified))

    for build_file in sorted(project_root.rglob("pom.xml")):
        original = build_file.read_text()
        modified = _plan_pom_xml(original)
        if modified is not None and modified != original:
            plans.append(BuildFileUpdate(file=build_file, original=original, modified=modified))

    for name in ("build.gradle", "build.gradle.kts"):
        for build_file in sorted(project_root.rglob(name)):
            original = build_file.read_text()
            modified = _plan_build_gradle(original)
            if modified is not None and modified != original:
                plans.append(BuildFileUpdate(file=build_file, original=original, modified=modified))

    for build_file in sorted(project_root.rglob("build.sbt")):
        original = build_file.read_text()
        modified = _plan_build_sbt(original)
        if modified is not None and modified != original:
            plans.append(BuildFileUpdate(file=build_file, original=original, modified=modified))

    return plans


def update_dependencies(project_root: Path) -> list[Path]:
    """Apply plans. Returns paths actually modified. Backward-compatible wrapper."""
    updated: list[Path] = []
    for plan in plan_dependencies_update(project_root):
        plan.file.write_text(plan.modified)
        updated.append(plan.file)
    return updated


def _plan_requirements_txt(content: str) -> str | None:
    if "pyiceberg" in content:
        return None
    return content.rstrip() + f"\n{PYICEBERG_REQ}\n"


def _plan_pyproject_toml(content: str) -> str | None:
    if "pyiceberg" in content:
        return None
    new_content = re.sub(
        r'(dependencies\s*=\s*\[)(.*?)(\])',
        lambda m: m.group(1) + m.group(2).rstrip() + f'\n  "{PYICEBERG_REQ}",\n' + m.group(3),
        content,
        flags=re.DOTALL,
    )
    if new_content == content:
        return None
    return new_content


def _plan_pom_xml(content: str) -> str | None:
    if ICEBERG_ARTIFACT in content:
        return None
    iceberg_dep = (
        f'        <dependency>\n'
        f'            <groupId>{ICEBERG_GROUP}</groupId>\n'
        f'            <artifactId>{ICEBERG_ARTIFACT}</artifactId>\n'
        f'            <version>{ICEBERG_VERSION}</version>\n'
        f'        </dependency>\n'
    )
    if '</dependencies>' in content:
        return content.replace('</dependencies>', iceberg_dep + '    </dependencies>', 1)
    block = f'    <dependencies>\n{iceberg_dep}    </dependencies>\n'
    new_content = content.replace('</project>', block + '</project>', 1)
    return new_content if new_content != content else None


def _plan_build_gradle(content: str) -> str | None:
    if ICEBERG_ARTIFACT in content:
        return None
    coordinate = f"'{ICEBERG_GROUP}:{ICEBERG_ARTIFACT}:{ICEBERG_VERSION}'"
    line = f"    implementation {coordinate}\n"
    if re.search(r'dependencies\s*\{', content):
        return re.sub(
            r'(dependencies\s*\{)',
            lambda m: m.group(1) + "\n" + line,
            content,
            count=1,
        )
    return content.rstrip() + f"\n\ndependencies {{\n{line}}}\n"


def _plan_build_sbt(content: str) -> str | None:
    if "iceberg-spark-runtime" in content:
        return None
    dep_line = (
        f'libraryDependencies += "{ICEBERG_GROUP}" %% "{ICEBERG_ARTIFACT_SBT}" % "{ICEBERG_VERSION}"\n'
    )
    return content.rstrip() + "\n\n" + dep_line
