package ldbc.driver.neo4j;

public class Organization {
        private final long organizationId;
        private final int year;

        public Organization( long organizationId, int year )
        {
            this.organizationId = organizationId;
            this.year = year;
        }

        public long organizationId()
        {
            return organizationId;
        }

        public int year()
        {
            return year;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

            Organization that = (Organization) o;

            if ( organizationId != that.organizationId )
            { return false; }
            if ( year != that.year )
            { return false; }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = (int) (organizationId ^ (organizationId >>> 32));
            result = 31 * result + year;
            return result;
        }

        @Override
        public String toString()
        {
            return "Organization{" +
                    "organizationId=" + organizationId +
                    ", year=" + year +
                    '}';
        }
    }