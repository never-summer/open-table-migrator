from pathlib import Path
from skills.open_table_migrator.deps import update_dependencies


def test_adds_pyiceberg_to_requirements_txt(tmp_path):
    req = tmp_path / "requirements.txt"
    req.write_text("pandas>=2.0.0\npyarrow>=14.0.0\n")
    update_dependencies(tmp_path)
    content = req.read_text()
    assert "pyiceberg" in content
    assert "pandas" in content


def test_does_not_duplicate_pyiceberg(tmp_path):
    req = tmp_path / "requirements.txt"
    req.write_text("pyiceberg[sql-sqlite]>=0.7.0\n")
    update_dependencies(tmp_path)
    assert req.read_text().count("pyiceberg") == 1


def test_adds_pyiceberg_to_pyproject_toml(tmp_path):
    ppt = tmp_path / "pyproject.toml"
    ppt.write_text('[project]\nname = "myapp"\ndependencies = [\n  "pandas",\n]\n')
    update_dependencies(tmp_path)
    content = ppt.read_text()
    assert "pyiceberg" in content


def test_no_deps_file_is_noop(tmp_path):
    update_dependencies(tmp_path)


def test_adds_iceberg_to_pom_xml(tmp_path):
    pom = tmp_path / "pom.xml"
    pom.write_text('''<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>job</artifactId>
    <version>1.0.0</version>
    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>3.5.0</version>
        </dependency>
    </dependencies>
</project>
''')
    update_dependencies(tmp_path)
    content = pom.read_text()
    assert "iceberg-spark-runtime" in content
    assert "org.apache.iceberg" in content
    assert "spark-sql_2.12" in content


def test_does_not_duplicate_iceberg_in_pom(tmp_path):
    pom = tmp_path / "pom.xml"
    pom.write_text('''<project>
    <dependencies>
        <dependency>
            <groupId>org.apache.iceberg</groupId>
            <artifactId>iceberg-spark-runtime-3.5_2.12</artifactId>
            <version>1.5.0</version>
        </dependency>
    </dependencies>
</project>
''')
    update_dependencies(tmp_path)
    assert pom.read_text().count("iceberg-spark-runtime") == 1


def test_adds_iceberg_to_build_gradle(tmp_path):
    gradle = tmp_path / "build.gradle"
    gradle.write_text('''plugins {
    id 'java'
}
repositories { mavenCentral() }
dependencies {
    implementation 'org.apache.spark:spark-sql_2.12:3.5.0'
}
''')
    update_dependencies(tmp_path)
    content = gradle.read_text()
    assert "iceberg-spark-runtime" in content
    assert "implementation" in content


def test_does_not_duplicate_iceberg_in_gradle(tmp_path):
    gradle = tmp_path / "build.gradle"
    gradle.write_text('''dependencies {
    implementation 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0'
}
''')
    update_dependencies(tmp_path)
    assert gradle.read_text().count("iceberg-spark-runtime") == 1
