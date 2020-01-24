package fr.xebia.ldi;

/**
 * Created by loicmdivad.
 */
public class KeyValuePair<K, V> {

    public final K key;
    public final V value;

    public KeyValuePair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public String toString() {
        return "{\"" + key.toString() + "\": \"" + value.toString() + "\"}";
    }

    public static <K, V> KeyValuePair<K, V> pair(K a, V b) {
        return new KeyValuePair<>(a, b);
    }
}
