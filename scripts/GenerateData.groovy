import java.util.Random

def file = new File(args[0])
int numWords = Integer.parseInt(args[1])
int numSets = Integer.parseInt(args[2])
String outFile = args[3]

String word
int count = 0
String[] words = new String[numWords]
def lines = file.readLines()
for (int i = 0; i < numWords; i++) {
	word = lines[i].split('\t')[0]
	words[count++] = word
}
Random wordRandom = new Random(65432)
Random wordCountRandom = new Random(21345)
String line
File out = new File(outFile)
for ( i in 0..numSets-1 ) {
	//A set with a random size between 5 and 50
	line = "${i}\t"
	count = wordCountRandom.nextInt(20-5) + 5
	count.times() {
		line += "${words[wordRandom.nextInt(numWords-1)]} "
	}
	out.append("${line}\n")
}
