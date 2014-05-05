package controller;

import java.util.Random;

import javax.servlet.http.HttpServletRequest;

import model.Model;
import model.ScheduleDAO;

import controller.Action;

public class IndexAction extends Action {
	ScheduleDAO scheduleDAO;
	Model model;

	public IndexAction(Model model) {
		this.scheduleDAO= model.getScheduleDAO();
		this.model = model;
	}

	@Override
	public String getName() {
		return "index.do";
	}

	@Override
	public String perform(HttpServletRequest request) {
		try {
			// when you need to initialize the DB, use this code
			// scheduleDAO.initializeDB();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "index.do";
	}
	
	
}
