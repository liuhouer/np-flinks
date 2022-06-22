package cn.northpark.flink.table_sql_api;

public class WordCountBean {
    public String word;
    public int counts;

    public WordCountBean() {
    }

    public WordCountBean(String word, int counts) {
        this.word = word;
        this.counts = counts;
    }

    public static WordCountBean of(String word, int counts) {
        return new WordCountBean(word, counts);
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", counts=" + counts +
                '}';
    }
}
