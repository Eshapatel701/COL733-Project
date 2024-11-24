
package benchmarks ; 

public class TimeStampedEvent {
    public String value ;
    public long entryTimestamp;
    public long exitTimestamp;

    public TimeStampedEvent(String value, long timestamp) {
        this.value = value;
        this.entryTimestamp = timestamp;
    }

    public void setExitTimestamp(long timestamp) {
        this.exitTimestamp = timestamp;
    }
}