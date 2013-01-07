Name:		python-txmongo
Version:	0.4.0
Release:	1%{?dist}
Summary:	Twisted bindings for MongoDB

Group:		Development/Languages
License:	Apache License, Version 2.0
URL:		https://github.com/chergert/txmongo
Source0:	https://github.com/chergert/txmongo/archive/txmongo-0.4.0.tar.gz

BuildArch:	noarch

BuildRequires:	python-devel
BuildRequires:	python-setuptools
BuildRequires:	python-twisted
BuildRequires:	python-pymongo
Requires:	python-twisted
Requires:	python-pymongo
Requires:	python-bson

%description
txmongo allows access to MongoDB from python-twisted.

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
* Mon Jan 07 2013 Christian Hergert <christian@hergert.me>
- initial package for Fedora
