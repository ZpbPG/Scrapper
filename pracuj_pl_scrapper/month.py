from enum import Enum

class Month(Enum):
    styczen = 1
    luty = 2
    marzec = 3
    kwiecien = 4
    maj = 5
    czerwiec = 6
    lipiec = 7
    sierpien = 8
    wrzesien = 9
    pazdziernik = 10
    listopad = 11
    grudzien = 12

    def __str__(self):
        return self.name