package net.xdob.jdbc.util;

public enum SqlType {
	DQL,      // 数据查询语言
	DML,      // 数据操作语言
	DDL,      // 数据定义语言
	DCL,      // 数据控制语言
	TCL,      // 事务控制语言
	OTHER,
	UNKNOWN;   // 未知类型
	/**
	 * 获取SQL语句的是否是修改语句
	 */
	public boolean isModification() {
		return this == DML || this == DDL || this == DCL;
	}
	/**
	 * 判断是否为DML语句
	 */
	public boolean isDML() {
		return this == DML;
	}
	/**
	 * 判断是否为DDL语句
	 */
	public boolean isDDL() {
		return this == DDL;
	}
	/**
	 * 判断是否为DCL语句
	 */
	public boolean isDCL() {
		return this == DCL;
	}
	/**
	 * 判断是否为TCL语句
	 */
	public boolean isTCL() {
		return this == TCL;
	}
	/**
	 * 判断是否为DQL语句
	 */
	public boolean isDQL() {
		return this == DQL;
	}
	/**
	 * 判断是否为其它语句
	 */
	public boolean isOther() {
		return this == OTHER;
	}
}
