package com.msd.gin.halyard.algebra;

import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Service;

public final class ServiceRoot extends QueryRoot {
    private static final long serialVersionUID = 7052207623408379003L;

    public static ServiceRoot create(Service service) {
        return new ServiceRoot(service);
    }

    private final Service service;

    private ServiceRoot(Service service) {
        super(service.getArg().clone());
        this.service = service;
    }

    public Service getService() {
    	return service;
    }

    @Override
	public long getResultSizeActual() {
    	synchronized (service) {
    		return service.getResultSizeActual();
    	}
	}

	@Override
	public void setResultSizeActual(long resultSizeActual) {
    	synchronized (service) {
    		service.setResultSizeActual(resultSizeActual);
    	}
	}
}
