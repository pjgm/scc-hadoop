import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordsCount implements WritableComparable<WordsCount> {

    private MapWritable wordsCounter;

    public WordsCount() {
        wordsCounter = new MapWritable();
    }

    public WordsCount(MapWritable wordsCounter) {
        this.wordsCounter = wordsCounter;
    }

    public void addWord(String word) {
        if (!wordsCounter.containsKey(word)) {
            Text w = new Text(word);
            IntWritable c = new IntWritable(1);
            wordsCounter.put(w, c);
        }
        else {
            IntWritable countWritable = (IntWritable) wordsCounter.get(word);
            int count = countWritable.get();
            wordsCounter.put(new Text(word), new IntWritable(++count));
        }
    }

    public MapWritable getWordsCounter() {
        return wordsCounter;
    }

    @Override
    public int compareTo(WordsCount wordsCount) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        wordsCounter.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        wordsCounter.readFields(dataInput);
    }

    @Override
    public String toString() {
        String result = "\n";
        for (MapWritable.Entry e: wordsCounter.entrySet()) {
            result += e.getKey().toString() + "\t" + e.getValue().toString() + "\n";
        }
        return result;
    }

}
