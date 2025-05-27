from typing import Dict


def main():
    dictity1 = [
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
    ]

    dictity2 = [
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
    ]

    dictity3 = [
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
        {"isi": "gas", "bakso": 1},
    ]

    dictity = {"d1": dictity1, "d2": dictity2, "d3": dictity3}
    jal = find_largest(dictity)
    print(jal)


def find_largest(nil: Dict) -> int:
    panjang = []
    naila = [*nil]
    for i in naila:
        panjang.append(len(nil[i]))
    maxi = max(panjang)
    posisi = panjang.index(maxi)
    pos = naila[posisi]
    return nil[pos]


if __name__ == "__main__":
    main()
