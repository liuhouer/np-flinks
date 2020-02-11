package cn.northpark.flink;

public class WordCount {
    public String word;
    public Integer counts;

    public WordCount() {
    }

    public WordCount(String word, Integer counts) {
        this.word = word;
        this.counts = counts;
    }

    public static WordCount of(String word, Integer counts) {
        return new WordCount(word, counts);
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", counts=" + counts +
                '}';
    }
}
