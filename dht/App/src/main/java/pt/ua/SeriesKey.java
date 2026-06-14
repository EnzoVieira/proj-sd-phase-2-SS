package pt.ua;

import java.time.LocalDate;
import java.util.Objects;

public final class SeriesKey {
    private final LocalDate day;
    private final String indexField;
    private final String indexValue;

    public SeriesKey(LocalDate day, String indexField, String indexValue) {
        this.day = day;
        this.indexField = indexField;
        this.indexValue = indexValue;
    }

    public LocalDate getDay() {
        return day;
    }

    public String getIndexField() {
        return indexField;
    }

    public String getIndexValue() {
        return indexValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SeriesKey)) return false;
        SeriesKey seriesKey = (SeriesKey) o;
        return Objects.equals(day, seriesKey.day)
                && Objects.equals(indexField, seriesKey.indexField)
                && Objects.equals(indexValue, seriesKey.indexValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(day, indexField, indexValue);
    }

    @Override
    public String toString() {
        return day + "|" + indexField + "|" + indexValue;
    }
}