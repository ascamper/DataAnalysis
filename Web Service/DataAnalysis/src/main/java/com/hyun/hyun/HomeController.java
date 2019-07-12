package com.hyun.hyun;

import java.io.IOException;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

/**
 * Handles requests for the application home page.
 */
@Controller
public class HomeController {

	private static final Logger logger = LoggerFactory.getLogger(HomeController.class);

	/**
	 * Simply selects the home view to render by returning its name.
	 */
//	@RequestMapping(value = "/", method = RequestMethod.GET)
//	public String home(Locale locale, Model model) {
//		logger.info("Welcome home! The client locale is {}.", locale);
//		
//		Date date = new Date();
//		DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG, locale);
//		
//		String formattedDate = dateFormat.format(date);
//		
//		model.addAttribute("serverTime", formattedDate );
//		
//		return "home";
//	}

	@RequestMapping(value = "/", method = RequestMethod.GET)
	public String parameterTable(Locale locale, Model model) {
		DBCommon<Parameter> dbCommon = new DBCommon<Parameter>("parameter_group4");
		model.addAttribute("table", dbCommon.selectDataTableTag(new Parameter()));
		model.addAttribute("parameter", dbCommon.selectColumnTableTag());
		return "parameterTable";
	}

	@RequestMapping(value = "/refact", method = RequestMethod.GET)
	public String changeParams(Locale locale, Model model, @RequestParam("parameter") String parameter,
			@RequestParam("value") String value, @RequestParam("button") String type,
			@RequestParam(name = "isChecked", required = false) String[] deleteParameter) {
		if (type.equals("Add Parameter")) {
			if (parameter.equals("average_window")) {
				if (Integer.valueOf(value) >= 3 && Integer.valueOf(value) <= 8) {
					DBCommon<Parameter> dbCommon = new DBCommon<Parameter>("parameter_group4");
					try {
						dbCommon.insertData(new Parameter(parameter, Integer.valueOf(value)));
					} catch (Exception e) {
						e.printStackTrace();
						// TODO: handle exception
					}
				}
			} else if (parameter.equals("predict_range")) {
				if (Integer.valueOf(value) >= 4 && Integer.valueOf(value) <= 8) {
					DBCommon<Parameter> dbCommon = new DBCommon<Parameter>("parameter_group4");
					try {
						dbCommon.insertData(new Parameter(parameter, Integer.valueOf(value)));
					} catch (Exception e) {
						e.printStackTrace();
						// TODO: handle exception
					}
				}
			} else if (parameter.equals("base_data")) {
				if (Integer.valueOf(value) >= 4 && Integer.valueOf(value) <= 20) {
					DBCommon<Parameter> dbCommon = new DBCommon<Parameter>("parameter_group4");
					try {
						dbCommon.insertData(new Parameter(parameter, Integer.valueOf(value)));
					} catch (Exception e) {
						e.printStackTrace();
						// TODO: handle exception
					}
				}
			}
		} else if (type.equals("Modify")) {
			if (parameter.equals("average_window")) {
				if (Integer.valueOf(value) >= 3 && Integer.valueOf(value) <= 8) {
					DBCommon<Parameter> dbCommon = new DBCommon<Parameter>("parameter_group4");
					try {
						dbCommon.updateData(parameter, value);
					} catch (Exception e) {
						e.printStackTrace();
						// TODO: handle exception
					}
				}
			} else if (parameter.equals("predict_range")) {
				if (Integer.valueOf(value) >= 4 && Integer.valueOf(value) <= 8) {
					DBCommon<Parameter> dbCommon = new DBCommon<Parameter>("parameter_group4");
					try {
						dbCommon.updateData(parameter, value);
					} catch (Exception e) {
						e.printStackTrace();
						// TODO: handle exception
					}
				}
			} else if (parameter.equals("base_data")) {
				if (Integer.valueOf(value) >= 4 && Integer.valueOf(value) <= 20) {
					DBCommon<Parameter> dbCommon = new DBCommon<Parameter>("parameter_group4");
					try {
						dbCommon.updateData(parameter, value);
					} catch (Exception e) {
						e.printStackTrace();
						// TODO: handle exception
					}
				}
			}
		} else if (type.equals("delete")) {
			System.out.println(deleteParameter);
			DBCommon<Parameter> dbCommon = new DBCommon<Parameter>("parameter_group4");
		}
		return "redirect:/";
	}

	@RequestMapping(value = "/table", method = RequestMethod.GET)
	public String goTable(Locale locale, Model model, @RequestParam(value = "mode", required = false) String mode) {
		Select select = new Select();
		model.addAttribute("item_list", select.setItemList());
		model.addAttribute("qty_data", select.getQtyList());
		if (mode == null) {
			DBCommon<Table> dbCommon = new DBCommon<Table>();
			model.addAttribute("select_result", dbCommon.selectDataTableTag2(new Table()));
		} else if (mode.equals("model")) {
			DBCommon<Table> dbCommon = new DBCommon<Table>();
			model.addAttribute("select_result", dbCommon.selectDataTableTag3(new Table()));

		} else {
			DBCommon<Table> dbCommon = new DBCommon<Table>();
			model.addAttribute("select_result", dbCommon.selectDataTableTag2(new Table()));
		}
		return "Table";
	}

	@RequestMapping(value = "/analysing", method = RequestMethod.GET)
	public String login(Locale locale, Model model) throws IOException {
		System.out.println("start");
		String host = "192.168.110.117";
		int port = 22;
		String username = "hadoop";
		String password = "1234";

		String command = "sh /home/hadoop/group4_2019/runAnalysis.sh";
		// String command = "touch /home/hadoop/group4_2019/test2019.txt";
//        String command = "spark-submit --class /home/hadoop/group4_2019/com.pjw.DataAnalysis kopoMavenProject-1.0.jar";

		System.out.println("==> Connecting to" + host);
		Session session = null;
		Channel channel = null;

		// 2. ���� ��ü�� �����Ѵ� (����� �̸�, ������ ȣ��Ʈ, ��Ʈ�� ���ڷ� �ش�.)
		try {
			// 1. JSch ��ü�� �����Ѵ�.
			JSch jsch = new JSch();
			session = jsch.getSession(username, host, port);

			// 3. �н����带 �����Ѵ�.
			session.setPassword(password);

			// 4. ���ǰ� ���õ� ������ �����Ѵ�.
			java.util.Properties config = new java.util.Properties();
			// 4-1. ȣ��Ʈ ������ �˻����� �ʴ´�.
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);

			// 5. �����Ѵ�.
			session.connect();

			// 6. sftp ä���� ����.
			channel = session.openChannel("exec");

			// 8. ä���� SSH�� ä�� ��ü�� ĳ�����Ѵ�
			ChannelExec channelExec = (ChannelExec) channel;

			System.out.println("==> Connected to" + host);

			channelExec.setCommand(command);
			channelExec.connect();

			System.out.println("==> Connected to" + host);

		} catch (JSchException e) {
			e.printStackTrace();
		} finally {
			if (channel != null) {
				channel.disconnect();
			}
			if (session != null) {
				session.disconnect();
			}
		}
		System.out.println("end");

		return "redirect:/";
	}

	@RequestMapping(value = "filter_data_table", method = RequestMethod.GET)
	public String filterDataTable(Locale locale, Model model, @RequestParam("seg1") String seg1,
			@RequestParam("seg2") String seg2, @RequestParam("product_group") String productGroup,
			@RequestParam("item") String item) {
		Select select = new Select();
		model.addAttribute("item_list", select.setItemList());
		System.out.println(seg1 + " " + seg2 + " " + productGroup + " " + item);
		DBCommon<Table> dbCommon = new DBCommon<Table>();
		model.addAttribute("select_result", dbCommon.selectDataTableTag4(new Table(), seg1, seg2, productGroup, item));
		model.addAttribute("qty_data", dbCommon.getQtyList(new Table(), seg1, seg2, productGroup, item));

		return "Table";
	}

}
