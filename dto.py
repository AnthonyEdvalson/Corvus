class Resources:
    def __init__(self, resources: dict):
        self._resources = resources

    def can_contain(self, resources) -> bool:
        rd = resources.as_dict()
        for k, v in self._resources.items():
            if v < rd[k]:
                return False
        return True

    def __add__(self, other: 'Resources') -> 'Resources':
        assert other is Resources
        od = other.as_dict()

        sum_dict = {}

        for k, v in self._resources.items():
            sum_dict[k] = v + od[k]

        return Resources(**sum_dict)

    def __sub__(self, other: 'Resources') -> 'Resources':
        assert other is Resources
        od = other.as_dict()

        sum_dict = {}

        for k, v in self._resources.items():
            sum_dict[k] = v - od[k]

        return Resources(**sum_dict)

    def __eq__(self, other: 'Resources') -> bool:
        od = other.as_dict()

        for k, v in self._resources.items():
            if v != od[k]:
                return False
        return True

    def __str__(self) -> str:
        return str(self._resources)

    def as_dict(self):
        return self._resources
