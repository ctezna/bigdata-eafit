##Salario promedio por sector
class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
	    text = line.split(",")
	    se = text[1]
	    salse = int(text[2])
	    yield se, salse

    def reducer(self, key, values):
	    lista = list(values)
	    prom = sum(lista) / len(lista)
        yield key, prom

if __name__ == '__main__':
    MRWordFrequencyCount.run()