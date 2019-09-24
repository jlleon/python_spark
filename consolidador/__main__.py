from data.consolidador_posicion import ConsolidadorPosicion
from data.consolidador_variacion import ConsolidadorVariacion

import sys

def main():
    if sys.argv.__len__() != 3:
        print("Falta el argumento <fecha> <tipo>")
        return
    
    if sys.argv[2] == "posicion":
        src = ConsolidadorPosicion(sys.argv[1])
        src.run()
    else:
        src = ConsolidadorVariacion(sys.argv[1])
        src.run()

if __name__ == "__main__":
    main()