package model;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

public class Model {
	// keep private instances of DAOs
	private ScheduleDAO scheduleDAO;

	public Model(ServletConfig config) throws ServletException {

		String jdbcDriver = config.getInitParameter("jdbcDriverName");
		String jdbcURL = config.getInitParameter("jdbcURL");
		
		// initialize connectional pool here
		
		// initialize DAO's here
		try {
			scheduleDAO = new ScheduleDAO(jdbcDriver, jdbcURL);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	// getDAO methods here
	public ScheduleDAO getScheduleDAO() {
		return scheduleDAO;
	}
}
