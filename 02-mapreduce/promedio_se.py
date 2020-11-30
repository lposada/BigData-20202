from mrjob.job import MRJob

class Promedio(MRJob):

    def mapper(self, _, line):
	line = line.replace(' ','')
	linea = line.split(',')
	se = linea[1]
	try:
		salario = int(linea[2])
	except ValueError:
		pass
	else:
		yield se,salario        


    def reducer(self, key, values):
	valores = list(values)
	avg = sum(valores)/len(valores)
	yield key,avg

if __name__ == '__main__':
    Promedio.run()
