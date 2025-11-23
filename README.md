## Integrantes:
- Jhossep Martinez / 202173530-5
- Fernando Xais / 202273551-1
- Gabriela Yáñez / 202273511-2

## Consideraciones:
- Las imágenes ya están compiladas pero se puede ejecutar el siguiente comando en cada vm para volver a compilarlas: ```make build-<mv1|mv2|mv3|mv4>``` dependiendo de la vm en la que este.
- En el directorio de "lab3" se encuentra el archivo "flight_updates.csv", desde aquí se leen la información de los vuelos y sus actualizaciones, en caso de querer usar otro archivo debe mantenerse su ubicación y nombre.
- El broker almacena el archivo "Reporte.txt" dentro del directorio "/output" dentro de su respectiva máquina.
- Si se cambia un archivo es necesario volver a ejecutar el build.
- Para poder ver los archivos generados dentro del directorio "/output" hay que ejecutar el siguiente comando en la máquina respectiva:
 
   ~~~
   cat <nombreArchivo>
   ~~~


## Instrucciones:
- Abrir una terminal para dist13, dist14, dist15 y dist16. Ingresar al directorio "/lab3" en cada terminal

- Ir a la VM dist13 y ejecutar ```make start-mv1```.
- Ir a la VM dist16 y ejecutar ```make start-mv4```.
- Ir a la VM dist14 y ejecutar ```make start-mv2```.
- Ir a la VM dist15 y ejecutar ```make start-mv3```.
- Se puede terminar con las ejecuciones con Ctrl+C, procurar terminar primero con la ejecución de la VM dist13 para evitar fallos en el reporte. Se pueden ver los archivos generados ingresando al directorio "/output" de la V, dist13.

## Credenciales VM:
### dist013
- ehe6gqRsS2Fk
- 10.35.168.23
  
**Entidades:**
- Broker
- Datanode 3
- Cliente RYW 1
- Cliente MR 1

### dist014
- KRZ65kfAEmpB
- 10.35.168.24

**Entidades:**
- Nodo Consenso 1
- Cliente RYW 2
- Cliente MR 2

### dist015
- aNASDGkYnQ8F
- 10.35.168.25

**Entidades:**
- Datanode 1
- Nodo Consenso 2
- Cliente RYW 3

### dist016
- jrKU59Umn2TW
- 10.35.168.26

**Entidades:**
- Coordinador
- Datanode 2
- Nodo Consenso 3

