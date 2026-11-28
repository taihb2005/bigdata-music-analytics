"""Install the pinned Delta/S3 JARs at image build time, avoiding runtime downloads."""
from pathlib import Path
from urllib.request import urlretrieve

import pyspark

JARS = (
    ("io/delta", "delta-spark_2.12", "3.3.3"),
    ("io/delta", "delta-storage", "3.3.3"),
    ("org/apache/hadoop", "hadoop-aws", "3.3.4"),
    ("com/amazonaws", "aws-java-sdk-bundle", "1.12.262"),
)

if __name__ == "__main__":
    destination = Path(pyspark.__file__).parent / "jars"
    for group, artifact, version in JARS:
        name = f"{artifact}-{version}.jar"
        urlretrieve(f"https://repo.maven.apache.org/maven2/{group}/{artifact}/{version}/{name}", destination / name)
