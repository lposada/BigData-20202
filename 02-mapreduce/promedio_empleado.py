from mrjob.job import MRJob

class Promedio(MRJob):

    def mapper(self, _, line):
	line = line.replace(' ','')
	linea = line.split(',')
	empleado = linea[0]
	try:
		salario = int(linea[2])
	except ValueError:
		pass
	else:
		yield empleado,salario        


    def reducer(self, key, values):
	valores = list(values)
	avg = sum(valores)/len(valores)
	yield key,avg

if __name__ == '__main__':
    Promedio.run()
