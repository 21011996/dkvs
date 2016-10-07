package utility;

import java.util.Scanner;

/**
 * Created by Ilya239 on 15.09.2016.
 */
public class Entry {

    public final Operation operation;
    public final String key;
    public final String value;
    public final int opNumber;

    public Entry(Operation operation, String key, String value, int opNumber) {
        this.operation = operation;
        this.key = key;
        this.value = value;
        this.opNumber = opNumber;
    }

    public static Entry parseEntry(Scanner sc) {
        return new Entry(Operation.valueOf(sc.next()), sc.next(), sc.next(), Integer.parseInt(sc.next()));
    }

    @Override
    public String toString() {
        return operation + " " + (key == null ? "?" : key) + " " + (value == null ? "?" : value) + " " + opNumber;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof Entry)) {
            return false;
        }
        Entry right = (Entry) other;
        return (operation == right.operation && key.equals(right.key) && value.equals(right.value) && opNumber == right.opNumber);
    }
}
