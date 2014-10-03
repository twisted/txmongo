Name:		python-txmongo
Version:	0.5.0
Release:	1%{?dist}
Summary:	Twisted driver for MongoDB

Group:		Development/Languages
License:	Apache License, Version 2.0
URL:		https://github.com/fiorix/mongo-async-python-driver
Source0:	https://github.com/fiorix/mongo-async-python-driver/archive/master.zip

BuildArch:	noarch

BuildRequires:	python-devel
BuildRequires:	python-setuptools
BuildRequires:	python-twisted
BuildRequires:	python-pymongo
Requires:	python-twisted
Requires:	python-pymongo
Requires:	python-bson

%description
txmongo is a Python/Twisted driver for MongoDB that implements the wire
protocol on non-blocking sockets. The API derives from the original pymongo.

%prep
%setup -q -n txmongo-txmongo-%{version}

%build
%{__python} setup.py build

%install
%{__python} setup.py install -O1 --skip-build --root $RPM_BUILD_ROOT

%files
%doc README.md
%doc COPYING

%{python_sitelib}/*

%changelog
* Thu Oct 02 2014 Alexandre Fiori <fiorix@gmail.com>
- Code review and cleanup
- Bug fixes

* Mon Jan 07 2013 Christian Hergert <christian@hergert.me>
- initial package for Fedora
