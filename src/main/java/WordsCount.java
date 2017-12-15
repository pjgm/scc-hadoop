import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WordsCount implements WritableComparable<WordsCount> {

    private MapWritable wordsCounter;

    public WordsCount() {
        wordsCounter = new MapWritable();
    }

    public WordsCount(MapWritable wordsCounter) {
        this.wordsCounter = wordsCounter;
    }

    public void addWord(String word) {

        Text wordText = new Text(word);

        if (!wordsCounter.containsKey(wordText)) {
            Text w = new Text(word);
            IntWritable c = new IntWritable(1);
            wordsCounter.put(w, c);
        }
        else {
            IntWritable countWritable = (IntWritable) wordsCounter.get(wordText);
            int count = countWritable.get();
            wordsCounter.put(wordText, new IntWritable(++count));
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

        List<MapWritable.Entry> topEntries = new ArrayList<>();

        for (int i = 0; i < 5; i++ ) {
            int highest = Integer.MIN_VALUE;
            MapWritable.Entry entry = null;
            for (MapWritable.Entry e : wordsCounter.entrySet()) {
                IntWritable val = (IntWritable) e.getValue();
                if (val.get() > highest) {
                    highest = val.get();
                    entry = e;
                }
            }
            topEntries.add(entry);
            if (entry != null) {
                wordsCounter.remove(entry.getKey());
            }
        }

        String result = "\n";

        for (MapWritable.Entry e: topEntries) {
            if (e == null)
                continue;
            result += e.getKey().toString() + "\t" + e.getValue().toString() + "\n";
        }
        return result;
    }

}
