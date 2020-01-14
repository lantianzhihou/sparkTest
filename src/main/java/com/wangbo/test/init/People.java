package com.wangbo.test.init;

//import javax.persistence.Transient;
import java.io.Serializable;

public class People implements Serializable {
	private static final long serialVersionUID = 5195838468627619128L;

	private String name;

	private Integer age;

	private transient Son son;

	public static Son obj;

	public People() {
	}

	public People(String name, Integer age) {
		this.name = name;
		this.age = age;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public Son getSon() {
		return son;
	}

	public void setSon(Son son) {
		this.son = son;
	}

}
