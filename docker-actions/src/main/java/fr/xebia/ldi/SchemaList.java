package fr.xebia.ldi;

import java.util.Collections;
import java.util.List;

/**
 * Created by loicmdivad.
 */
public class SchemaList {

    private List<SubjectEntry> schemas = Collections.emptyList();

    public void setSchemas(List<SubjectEntry> schemas) {
        this.schemas = schemas;
    }

    public List<SubjectEntry> getSchemas() {
        return schemas;
    }

    public static class SubjectEntry {

        private String file;
        private String subject;

        public String getFile() {
            return file;
        }

        public String getSubject() {
            return subject;
        }

        public void setFile(String file) {
            this.file = file;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }
    }
}
