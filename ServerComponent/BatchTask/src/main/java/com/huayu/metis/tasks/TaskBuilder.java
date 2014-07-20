package com.huayu.metis.tasks;

import com.huayu.metis.options.TaskOption;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * 执行Task的构建器
 * Created by Administrator on 14-5-15.
 */
public class TaskBuilder {

    private TaskOption option;

    public TaskBuilder(TaskOption option) {
        this.option = option;
    }

    public void loadFromJar() throws Exception {
        File jarFile = new File(option.getPackagePath());
        URLClassLoader loader = new URLClassLoader(new URL[]{jarFile.toURI().toURL()});
        try {
            Class<?> cls = loader.loadClass(option.getTaskClass());
            //如果任务类不是MetisBasicTask的子类
            if(cls.getSuperclass() != BasicTask.class) {
                //TODO: 出错处理
                return;
            }
            this.option.setExecuteTaskClass((Class<? extends BasicTask>) cls);
        } catch (ClassNotFoundException cfe) {

        }
    }

    public BasicTask build() throws Exception {
        Class<? extends BasicTask> executeCls = this.option.getExecuteTaskClass();
        if(executeCls == null) {
            throw new NullPointerException("execute class is null");
        }

        try {
            Constructor constructor = executeCls.getDeclaredConstructor(new Class[] {TaskOption.class});
            constructor.setAccessible(true);
            return (BasicTask)constructor.newInstance(new Object[] {this.option});
        } catch (InstantiationException e) {
            throw e;
        } catch (IllegalAccessException e) {
            throw e;
        }
    }
}
