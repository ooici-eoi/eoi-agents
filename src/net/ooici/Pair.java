/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici;


/**
 * Generic container for any immutable, arbitrary pair of values of type <code>K</code> and <code>V</code>. (Think something akin to
 * java.util.Map.Entry<K,V>)
 * 
 * @author tlarocque
 * 
 * @param <K> The class-type for this Pair's key portion
 * @param <V> The class-type for this Pair's value portion
 */
public class Pair<K, V> {

    protected K key;
    protected V value;
    private int hash = -1;

    /**
     * Privitized constructor to prevent external instantiation.<br />
     * Constructs an new empty Pair object.
     */
    Pair() {
    }

    /**
     * Constructs a new Pair Object, mapping the given key to the given value
     * 
     * @param key any Object of type <code>K</code>
     * @param value any Object of type <code>V</code>
     */
    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    /**
     * @return Retrieves this pairing's key portion
     */
    public K getKey() {
        return key;
    }

    /**
     * @return Retrieves this pairing's value portion
     */
    public V getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        /* Lazy-initialize the hash for this immutable Pair */
        /* Values 7, 11, 13, and 31 below are arbitrary prime values which force uniqueness of the objects contained in this pair */
        if (hash == -1) {
            hash = 0;

            /* This forces null values to be regarded as unique values in hashtables */
            int h1 = (key != null) ? (key.hashCode() * 7) : (13);
            int h2 = (value != null) ? (value.hashCode() * 11) : (13);

            /* This makes the arrangement of values unique such that if h1 != h2 ... Pair{h1, h2} != Pair{h2, h1} */
            hash = (h1 * 31) + h2;
        }

        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Pair<?, ?>) ? (hashCode() == obj.hashCode()) : (false);
    }
}
