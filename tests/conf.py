import os
from typing import Optional, List


class MongoConf:

    def __init__(self):
        super().__init__()

    @property
    def run_in_docker(self) -> bool:
        return os.environ.get("TXMONGO_RUN_MONGOD_IN_DOCKER") == "yes"

    @property
    def version(self) -> str:
        _version = os.environ["TXMONGO_MONGOD_DOCKER_VERSION"]
        if not _version:
            raise NotImplemented("Need to set mongo version!")
        return _version

    @property
    def network(self) -> Optional[str]:
        return os.environ.get("TXMONGO_MONGOD_DOCKER_NETWORK_NAME")

    @property
    def auth_test_port(self) -> int:
        return int(os.environ.get("TXMONGO_MONGOD_DOCKER_PORT_AUTH", 27018))

    @property
    def replicase_test_ports(self) -> List[int]:
        return list(
            map(
                int,
                {
                    os.environ.get("TXMONGO_MONGOD_DOCKER_PORT_1", 37017),
                    os.environ.get("TXMONGO_MONGOD_DOCKER_PORT_2", 37018),
                    os.environ.get("TXMONGO_MONGOD_DOCKER_PORT_3", 37019),
                },
            )
        )
