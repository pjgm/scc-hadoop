import java.util.HashMap;
import java.util.Map;

public class WordsCount {

    private Map<String, Integer> wordsCounter;

    public WordsCount() {
        wordsCounter = new HashMap<>();
    }

    public void addWord(String word) {
        if (!wordsCounter.containsKey(word))
            wordsCounter.put(word, 1);
        else {
            int count = wordsCounter.get(word);
            wordsCounter.put(word, ++count);
        }
    }

    public Map<String, Integer> getTopWords() {
        wordsCounter.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed()).limit(5).forEach(System.out::println);
        return null;
    }
}
