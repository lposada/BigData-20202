from mrjob.job import MRJob

class Promedio(MRJob):

    def mapper(self, _, line):
	line = line.replace(' ','')
	linea = line.split(',')
	se = linea[1]
	empleado = linea[0]
	try:
		salario = int(linea[2])
	except ValueError:
		pass
	else:
		yield empleado,se        


    def reducer(self, key, values):
	valores = list(values)
	numero_sectores = len(valores)
	yield key,numero_sectores

if __name__ == '__main__':
    Promedio.run()
