import java.util.regex.*;

public class Event {
    private String date;
    private String type;
    private String message;
    private EventId eventId;

    public enum EventId {
        E1("jk2_init\\(\\) Found child \\S+ in scoreboard slot \\S+"), E2("workerEnv\\.init\\(\\) ok \\S+"),
        E3("mod_jk child workerEnv in error state \\S+"),
        E4("\\[client \\S+\\] Directory index forbidden by rule: \\S+"),
        E5("jk2_init\\(\\) Can't find child \\S+ in scoreboard"), E6("mod_jk child init \\S+ \\S+");

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
            date = matcher.group(1);
            type = matcher.group(2);
            message = matcher.group(3);
            for (EventId eventType : EventId.values()) {
                if (message.matches(eventType.getRegex())) {
                    eventId = eventType;
                    break;
                }
            }
        }
    }

    public Boolean isParsed() {
        if (date == null || type == null || message == null || eventId == null) {
            return false;
        }
        return true;
    }

    public String getDate() {
        return date;
    }

    public String getMessage() {
        return message;
    }

    public String getType() {
        return type;
    }

    public EventId gerEventId() {
        return eventId;
    }
}
