package pl.edu.agh.csg;

import java.util.Objects;

public class NewVmArrival {

    private final String type;
    private final Double arrivalTimestamp;

    public NewVmArrival(String type, Double arrivalTimestamp) {
        this.type = type;
        this.arrivalTimestamp = arrivalTimestamp;
    }

    public String getType() {
        return type;
    }

    public Double getArrivalTimestamp() {
        return arrivalTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NewVmArrival that = (NewVmArrival) o;
        return Objects.equals(getType(), that.getType()) &&
                Objects.equals(getArrivalTimestamp(), that.getArrivalTimestamp());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), getArrivalTimestamp());
    }
}
