import java.text.DecimalFormat


def file = new File(args[0])
def out = new File(args[1])
DecimalFormat numF = new DecimalFormat('#.##')
def sets = []
file.withReader { reader ->
     while ((line = reader.readLine()) != null) {
     	sets.add(line.split('\t')[1])
     }
  }

println "Querying data"
long compares
Set<String> mainSet
Set<String> candidateSet
JaccardSimilarityScore similarity
for (int i = 0; i < sets.size(); i++) {
	mainSet = toSet(sets[i])
	compares = 0
	for (int j = i; j < sets.size(); j++) {
		compares++
		candidateSet = toSet(sets[j])
		similarity = jaccardSimilarity(mainSet, candidateSet)
		if (similarity.score >= 0.1) {
			out.append("${i}\t${j}\t${numF.format(similarity.score)}\n")
		}
	}
	println compares
}

Set<String> toSet(String set) {
	Set<String> newSet = new HashSet<String>()
	set.split(' ').each{ word ->
		newSet.add(word)
	}
	return newSet
}

JaccardSimilarityScore jaccardSimilarity(Set<String> a, Set<String> b) {
	int found = 0, notFound = 0
	Set<String> newSet = new HashSet<String>(a)
	newSet.addAll(b)
	newSet.each{ word -> 
		if (b.contains(word) && a.contains(word)) {
			found ++
		} else {
			notFound ++
		}
	}
	return new JaccardSimilarityScore(found, notFound)
}

class JaccardSimilarityScore {
	String visualScore
	double score

	JaccardSimilarityScore(int found, int notFound) {
		def denom = (found + notFound)
		if (found > 0 && denom > 0) {
			score = found/denom
		} else {
			score = 0
		}
		visualScore = "${found}/${denom}"
	}

	@Override
	public String toString() {
		return visualScore
	}
}

