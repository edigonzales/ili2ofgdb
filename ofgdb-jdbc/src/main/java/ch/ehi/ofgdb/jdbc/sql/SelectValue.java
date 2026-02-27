package ch.ehi.ofgdb.jdbc.sql;

public abstract class SelectValue {

	public abstract String getColumnName();

	@Override
	public String toString() {
		return "SelectValue [getColumnName()=" + getColumnName() + "]";
	}

}
