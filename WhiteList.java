package com.taobao.matrix.subsoil.web.home.screen.suspect;

import java.util.List;

import org.apache.log4j.Logger;

import com.alibaba.service.template.TemplateContext;
import com.alibaba.turbine.service.rundata.RunData;
import com.alibaba.webx.WebxException;
import com.taobao.matrix.crust.domain.SuspectQuery;
import com.taobao.matrix.crust.domain.dataobject.SuspectWhiteUserDO;
import com.taobao.matrix.crust.service.SuspectUserService;
import com.taobao.matrix.ring.auth.context.AuthProvider;
import com.taobao.matrix.subsoil.web.home.screen.BaseScreen;
import com.taobao.matrix.subsoil.web.home.screen.userlevel.UserRolePropertyScreen;

public class WhiteList extends BaseScreen {
	private static Logger logger = Logger.getLogger(WhiteList.class);
	private AuthProvider authProvider = null;
	private SuspectUserService suspectUserService;

	@Override
	protected void execute(RunData rundata, TemplateContext context)
			throws WebxException {
		if (!getAuthProvider().hasPermission("suspect_user_view")) {
			rundata.setRedirectTarget("/forbidden.vm");
			return;
		}

		try {
			String type = rundata.getParameters().getString("type", "1,2,3,4,5");
			int[] types = null;
			if (type != null) {
				String[] values = type.split(",");
				types = new int[values.length];
				for (int i = 0; i < values.length; i++) {
					types[i] = Integer.parseInt(values[i]);
				}
			}
			int page = rundata.getParameters().getInt("page", 1);
			int limit = 30;
			int start = (page-1)*limit;
			
			List<SuspectWhiteUserDO> list = suspectUserService.getWhiteUsers(types, start, limit);
			context.put("list", list);
		}
		catch (Exception e) {
			
		}
	}

	public void setAuthProvider(AuthProvider authProvider) {
		this.authProvider = authProvider;
	}

	public AuthProvider getAuthProvider() {
		return authProvider;
	}

	public void setSuspectUserService(SuspectUserService suspectUserService) {
		this.suspectUserService = suspectUserService;
	}

	public SuspectUserService getSuspectUserService() {
		return suspectUserService;
	}
}
