package com.msd.gin.halyard.jmx;

import java.text.DateFormat;
import java.util.Date;

import javax.management.openmbean.CompositeData;

import org.apache.commons.text.StringEscapeUtils;
import org.jminix.type.AttributeFilter;
import org.jminix.type.HtmlContent;

import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.strategy.TrackingThreadPoolExecutorMXBean;

public class JMiniXAttributeRenderer implements AttributeFilter {

	private static String escape(Object o) {
		return (o != null) ? StringEscapeUtils.escapeHtml4((String) o) : "";
	}

	@Override
	public Object filter(Object object) {
		if (object instanceof CompositeData[]) {
			CompositeData[] arr = (CompositeData[]) object;
			if (arr.length > 0) {
				String compositeType = arr[0].getCompositeType().getTypeName();
				if (HBaseSail.QueryInfo.class.getName().equals(compositeType)) {
					DateFormat df = DateFormat.getDateTimeInstance();
					StringBuilder s = new StringBuilder(1024);
					s.append("<ol>\n");
					for (CompositeData cd : arr) {
						s.append("<li>\n");
						s.append("<p>running: ").append(cd.get("running")).append("</p>\n");
						s.append("<p>startTimestamp: ").append(df.format(new Date((Long) cd.get("startTimestamp")))).append("</p>\n");
						Long endTimestamp = (Long) cd.get("endTimestamp");
						if (endTimestamp != null) {
							s.append("<p>endTimestamp: ").append(df.format(new Date(endTimestamp))).append("</p>\n");
						}
						s.append("<p>connectionId: ").append(cd.get("connectionId")).append("</p>\n");
						s.append("<p>queryString: <pre>").append(escape(cd.get("queryString"))).append("</pre></p>\n");
						s.append("<p>queryTree: <pre>").append(escape(cd.get("queryTree"))).append("</pre></p>\n");
						s.append("<p>optimizedQueryTree: <pre>").append(escape(cd.get("optimizedQueryTree"))).append("</pre></p>\n");
						s.append("</li>\n");
					}
					s.append("</ol>\n");
					return new Html(s.toString());
				} else if (TrackingThreadPoolExecutorMXBean.ThreadInfo.class.getName().equals(compositeType)) {
					StringBuilder s = new StringBuilder(1024);
					s.append("<ol>\n");
					for (CompositeData cd : arr) {
						s.append("<li>\n");
						s.append("<p>name: ").append(cd.get("name")).append("</p>\n");
						s.append("<p>state: ").append(cd.get("state")).append("</p>\n");
						s.append("<p>task: <pre>").append(escape(cd.get("task"))).append("</pre></p>\n");
						s.append("</li>\n");
					}
					s.append("</ol>\n");
					return new Html(s.toString());
				} else if (TrackingThreadPoolExecutorMXBean.QueueInfo.class.getName().equals(compositeType)) {
					StringBuilder s = new StringBuilder(1024);
					s.append("<ol>\n");
					for (CompositeData cd : arr) {
						s.append("<li>\n");
						s.append("<p>task: <pre>").append(escape(cd.get("task"))).append("</pre></p>\n");
						s.append("</li>\n");
					}
					s.append("</ol>\n");
					return new Html(s.toString());
				}
			}
		}
		return object;
	}

	public static final class Html implements HtmlContent {
		private final String html;
		public Html(String html) {
			this.html = html;
		}
		@Override
		public String toString() {
			return html;
		}
	}
}
