import java.util.regex.*;


/**
 * The Event class represents a log event with a specific time, level, content, and event ID.
 * It parses a log line to extract these details and determine the event ID based on predefined patterns.
 */
public class Event {
    private String time;
    private String level;
    private String content;
    private EventId eventId;

    /**
     * Enum representing various event identifiers with their associated regular expressions.
     * Each event identifier corresponds to a specific log pattern.
     */
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

    /**
     * Constructs an Event object by parsing a log line.
     *
     * @param line the log line to be parsed
     * 
     * The log line is expected to be in the format:
     * [time] [level] content
     * 
     * The constructor uses a regular expression to extract these components
     * and assigns them to the corresponding fields. It also attempts to match
     * the content against predefined event types (EventId) and assigns the
     * matching event type to the eventId field.
     */
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

    /**
     * Checks if the event has been parsed successfully.
     * 
     * @return true if all required fields (time, level, content, eventId) are not null, false otherwise.
     */
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
}
