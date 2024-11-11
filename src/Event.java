import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.*;

import org.apache.hadoop.io.Writable;

public class Event implements Writable {
    private String time;
    private String level;
    private String content;
    private EventId eventId;

    public enum EventId {
        E1("jk2_init\\(\\) Found child \\S+ in scoreboard slot \\S+"), E2("workerEnv\\.init\\(\\) ok \\S+"),
        E3("mod_jk child workerEnv in error state \\S+"),
        E4("\\[client \\S+\\] Directory index forbidden by rule: \\S+"),
        E5("jk2_init\\(\\) Can't find child \\S+ in scoreboard"), E6("mod_jk child init \\S+ \\S+"), OTHER(".*");

        private final String regex;

        EventId(String regex) {
            this.regex = regex;
        }

        public String getRegex() {
            return regex;
        }

    }

    Event(String line) {
        final Pattern pattern = Pattern.compile("\\[(.*?)\\] \\[(.*?)\\] (.*)");
        final Matcher matcher = pattern.matcher(line);

        if (matcher.find()) {
            time = matcher.group(1);
            level = matcher.group(2);
            content = matcher.group(3);
            for (EventId eventType : EventId.values()) {
                if (content.matches(eventType.getRegex())) {
                    eventId = eventType;
                    break;
                }
            }
        }
    }

    public Boolean isParsed() {
        if (time == null || level == null || content == null || eventId == null) {
            return false;
        }
        return true;
    }

    public String getTime() {
        return time;
    }

    public String getContent() {
        return content;
    }

    public String getLevel() {
        return level;
    }

    public EventId gerEventId() {
        return eventId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(time);
        out.writeUTF(level);
        out.writeUTF(content);
        out.writeUTF(eventId.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        time = in.readUTF();
        level = in.readUTF();
        content = in.readUTF();
        eventId = EventId.valueOf(in.readUTF());
    }

    @Override
    public String toString() {
        return isParsed()
                ? time + "," + level + "," + content + "," + eventId.name() + ","
                        + eventId.getRegex().replaceAll("\\\\S\\+", "<*>")
                : "Unparsed";
    }
}
