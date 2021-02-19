package nextflow

import com.zaxxer.hikari.HikariDataSource

import javax.sql.DataSource

class TaskDB {


    private static HikariDataSource ds = new HikariDataSource();

    private TaskDB() {

    }

    static {
        ds.setJdbcUrl("jdbc:postgresql://<ip>:5432/<dbName>");
        ds.setUsername("username");
        ds.setPassword("pw");
        ds.setDriverClassName('org.postgresql.Driver')
    }

    public static DataSource getDataSource() {
        return ds
    }

}
