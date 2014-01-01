package initial;
import java.util.Map;

/**
 * MyEntry for TreeSet
 * @author cindyzhang
 *
 * @param <K>
 * @param <V>
 */
public class MyEntry<K, V> implements Map.Entry<K, V> {
    private final K key;
    private V value;

    public MyEntry() {
        this.key = null;
        this.value = null;
    }
    
    public MyEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        V old = this.value;
        this.value = value;
        return old;
    }
}
