
package benchmarks ; 

public class TimeStampedEvent {
    public String value ;
    public long timestamp;

    public TimeStampedEvent(String value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }
}