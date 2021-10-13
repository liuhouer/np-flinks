package cn.northpark.flink.util;


import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.utils.AbstractParameterTool;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhangyang
 * 顺序读取配置文件
 * @date 2020年12月01日 17:40:52
 */
public class RichParameterTool extends AbstractParameterTool {



    private static final long serialVersionUID = 1L;

    // ------------------ Constructors ------------------------

    /**
     * Returns {@link ParameterTool} for the given arguments. The arguments are keys followed by values.
     * Keys have to start with '-' or '--'
     *
     * <p><strong>Example arguments:</strong>
     * --key1 value1 --key2 value2 -key3 value3
     *
     * @param args Input array arguments
     * @return A {@link ParameterTool}
     */
    public static RichParameterTool fromArgs(String[] args) {
        final Map<String, String> map = new HashMap<>(args.length / 2);

        int i = 0;
        while (i < args.length) {
            final String key = Utils.getKeyFromArgs(args, i);

            if (key.isEmpty()) {
                throw new IllegalArgumentException(
                        "The input " + Arrays.toString(args) + " contains an empty argument");
            }

            i += 1; // try to find the value

            if (i >= args.length) {
                map.put(key, NO_VALUE_KEY);
            } else if (NumberUtils.isNumber(args[i])) {
                map.put(key, args[i]);
                i += 1;
            } else if (args[i].startsWith("--") || args[i].startsWith("-")) {
                // the argument cannot be a negative number because we checked earlier
                // -> the next argument is a parameter name
                map.put(key, NO_VALUE_KEY);
            } else {
                map.put(key, args[i]);
                i += 1;
            }
        }

        return fromMap(map);
    }

    /**
     * Returns {@link RichParameterTool} for the given {@link Properties} file.
     *
     * @param path Path to the properties file
     * @return A {@link RichParameterTool}
     * @throws IOException If the file does not exist
     * @see Properties
     */
    public static RichParameterTool fromPropertiesFile(String path) throws IOException {
        File propertiesFile = new File(path);
        return fromPropertiesFile(propertiesFile);
    }

    /**
     * Returns {@link RichParameterTool} for the given {@link Properties} file.
     *
     * @param file File object to the properties file
     * @return A {@link RichParameterTool}
     * @throws IOException If the file does not exist
     * @see Properties
     */
    public static RichParameterTool fromPropertiesFile(File file) throws IOException {
        if (!file.exists()) {
            throw new FileNotFoundException("Properties file " + file.getAbsolutePath() + " does not exist");
        }
        try (FileInputStream fis = new FileInputStream(file)) {
            return fromPropertiesFile(fis);
        }
    }

    /**
     * Returns {@link RichParameterTool} for the given InputStream from {@link Properties} file.
     *
     * @param inputStream InputStream from the properties file
     * @return A {@link RichParameterTool}
     * @throws IOException If the file does not exist
     * @see Properties
     */
    public static RichParameterTool fromPropertiesFile(InputStream inputStream) throws IOException {
        Properties props = new OrderedProperties();
        props.load(inputStream);

        LinkedHashMap map = new LinkedHashMap();
        for (String key : props.stringPropertyNames()) {
            map.put(key,props.getProperty(key));
        }
        return fromMap(map);
    }

    /**
     * Returns {@link RichParameterTool} for the given map.
     *
     * @param map A map of arguments. Both Key and Value have to be Strings
     * @return A {@link RichParameterTool}
     */
    public static RichParameterTool fromMap(Map<String, String> map) {
        Preconditions.checkNotNull(map, "Unable to initialize from empty map");
        return new RichParameterTool(map);
    }

    /**
     * Returns {@link RichParameterTool} from the system properties.
     * Example on how to pass system properties:
     * -Dkey1=value1 -Dkey2=value2
     *
     * @return A {@link RichParameterTool}
     */
    public static RichParameterTool fromSystemProperties() {
        return fromMap((Map) System.getProperties());
    }

    // ------------------ ParameterUtil  ------------------------
    protected final Map<String, String> data;

    private RichParameterTool(Map<String, String> data) {
        this.data = Collections.unmodifiableMap(new LinkedHashMap<>(data));

        this.defaultData = new ConcurrentHashMap<>(data.size());

        this.unrequestedParameters = Collections.newSetFromMap(new ConcurrentHashMap<>(data.size()));

        unrequestedParameters.addAll(data.keySet());
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RichParameterTool that = (RichParameterTool) o;
        return Objects.equals(data, that.data) &&
                Objects.equals(defaultData, that.defaultData) &&
                Objects.equals(unrequestedParameters, that.unrequestedParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, defaultData, unrequestedParameters);
    }

    // ------------------ Get data from the util ----------------

    /**
     * Returns number of parameters in {@link RichParameterTool}.
     */
    @Override
    public int getNumberOfParameters() {
        return data.size();
    }

    /**
     * Returns the String value for the given key.
     * If the key does not exist it will return null.
     */
    @Override
    public String get(String key) {
        addToDefaults(key, null);
        unrequestedParameters.remove(key);
        return data.get(key);
    }

    /**
     * Check if value is set.
     */
    @Override
    public boolean has(String value) {
        addToDefaults(value, null);
        unrequestedParameters.remove(value);
        return data.containsKey(value);
    }

    // ------------------------- Export to different targets -------------------------

    /**
     * Returns a {@link Configuration} object from this {@link RichParameterTool}.
     *
     * @return A {@link Configuration}
     */
    public Configuration getConfiguration() {
        final Configuration conf = new Configuration();
        for (Map.Entry<String, String> entry : data.entrySet()) {
            conf.setString(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    /**
     * Returns a {@link Properties} object from this {@link RichParameterTool}.
     *
     * @return A {@link Properties}
     */
    public Properties getProperties() {
        Properties props = new OrderedProperties();
        props.putAll(this.data);
        return props;
    }

    /**
     * Create a properties file with all the known parameters (call after the last get*() call).
     * Set the default value, if available.
     *
     * <p>Use this method to create a properties file skeleton.
     *
     * @param pathToFile Location of the default properties file.
     */
    public void createPropertiesFile(String pathToFile) throws IOException {
        createPropertiesFile(pathToFile, true);
    }

    /**
     * Create a properties file with all the known parameters (call after the last get*() call).
     * Set the default value, if overwrite is true.
     *
     * @param pathToFile Location of the default properties file.
     * @param overwrite Boolean flag indicating whether or not to overwrite the file
     * @throws IOException If overwrite is not allowed and the file exists
     */
    public void createPropertiesFile(String pathToFile, boolean overwrite) throws IOException {
        final File file = new File(pathToFile);
        if (file.exists()) {
            if (overwrite) {
                file.delete();
            } else {
                throw new RuntimeException("File " + pathToFile + " exists and overwriting is not allowed");
            }
        }
        final Properties defaultProps = new OrderedProperties();
        defaultProps.putAll(this.defaultData);
        try (final OutputStream out = new FileOutputStream(file)) {
            defaultProps.store(out, "Default file created by Flink's ParameterUtil.createPropertiesFile()");
        }
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return new RichParameterTool(this.data);
    }

    // ------------------------- Interaction with other ParameterUtils -------------------------

    /**
     * Merges two {@link RichParameterTool}.
     *
     * @param other Other {@link RichParameterTool} object
     * @return The Merged {@link RichParameterTool}
     */
    public RichParameterTool mergeWith(RichParameterTool other) {
        final Map<String, String> resultData = new HashMap<>(data.size() + other.data.size());
        resultData.putAll(data);
        resultData.putAll(other.data);

        final RichParameterTool ret = new RichParameterTool(resultData);

        final HashSet<String> requestedParametersLeft = new HashSet<>(data.keySet());
        requestedParametersLeft.removeAll(unrequestedParameters);

        final HashSet<String> requestedParametersRight = new HashSet<>(other.data.keySet());
        requestedParametersRight.removeAll(other.unrequestedParameters);

        ret.unrequestedParameters.removeAll(requestedParametersLeft);
        ret.unrequestedParameters.removeAll(requestedParametersRight);

        return ret;
    }

    // ------------------------- ExecutionConfig.UserConfig interface -------------------------

    @Override
    public Map<String, String> toMap() {
        return data;
    }

    // ------------------------- Serialization ---------------------------------------------

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        defaultData = new ConcurrentHashMap<>(data.size());
        unrequestedParameters = Collections.newSetFromMap(new ConcurrentHashMap<>(data.size()));
    }

    /**
     * @author zhangyang
     * 顺序的property
     */
    static class OrderedProperties extends Properties {
        private static final long serialVersionUID = -4627607243846121965L;

        /**
         * 因为LinkedHashSet有序，所以，key在调用put()的时候，存放到这里也就有序。
         */
        private final LinkedHashSet<Object> keys = new LinkedHashSet<>();

        @Override
        public Enumeration<Object> keys() {
            return Collections.enumeration(keys);
        }

        /**
         * 在put的时候，只是把key有序的存到{@link OrderedProperties#keys}
         * 取值的时候，根据有序的keys，可以有序的取出所有value
         * 依然调用父类的put方法,也就是key value 键值对还是存在hashTable里.
         * 只是现在多了个存key的属性{@link OrderedProperties#keys}
         */
        @Override
        public Object put(Object key, Object value) {
            keys.add(key);
            return super.put(key, value);
        }

        /**
         * 因为复写了这个方法，在（方式一）的时候,才输出有序。
         */
        @Override
        public Set<String> stringPropertyNames() {
            Set<String> set = new LinkedHashSet<>();
            for (Object key : this.keys) {
                set.add((String) key);
            }
            return set;
        }

        /**
         * 因为复写了这个方法，在（方式二）的时候,才输出有序。
         */
        @Override
        public Set<Object> keySet() {
            return keys;
        }

        //这个就不设置有序了，因为涉及到HashTable内部类：EntrySet，不好复写。
        //public LinkedHashSet<Map.Entry<Object, Object>> entrySet() {
        //  LinkedHashSet<Map.Entry<Object, Object>> entrySet = new LinkedHashSet<>();
        //  for (Object key : keys) {
        //
        //  }
        //  return entrySet;
        //}

        /**
         * 因为复写了这个方法，在（方式四）的时候,才输出有序。
         */
        @Override
        public Enumeration<?> propertyNames() {
            return Collections.enumeration(keys);
        }
    }


}
