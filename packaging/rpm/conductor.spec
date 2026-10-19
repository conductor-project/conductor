%global debug_package %{nil}
# Preserve the exact prebuilt executable in the package payload. The default
# RPM install-post hooks strip binaries and rewrite ELF notes in the buildroot.
%global __os_install_post %{nil}
Name: conductor
Version: %{package_version}
Release: 1
Summary: Conductor job and service manager
License: Apache-2.0
URL: https://github.com/conductor-project/conductor
Source0: conductor-package.tar.gz
BuildArch: %{package_arch}
Requires(pre): shadow-utils
Requires: systemd

%description
Daemon for submitting one-shot jobs and managing service deployments on hosts operated by the administrator.

%prep
%setup -q -n root

%build

%install
mkdir -p %{buildroot}/usr/bin %{buildroot}/usr/lib/systemd/system
mkdir -p %{buildroot}/usr/share/doc/conductor/examples %{buildroot}/usr/share/licenses/conductor
install -m 0755 usr/bin/conductor %{buildroot}/usr/bin/conductor
install -m 0644 usr/lib/systemd/system/conductor.service %{buildroot}/usr/lib/systemd/system/conductor.service
install -m 0644 usr/share/doc/conductor/INSTALL.md %{buildroot}/usr/share/doc/conductor/INSTALL.md
install -m 0644 usr/share/doc/conductor/examples/agent.json %{buildroot}/usr/share/doc/conductor/examples/agent.json
install -m 0644 usr/share/doc/conductor/copyright %{buildroot}/usr/share/licenses/conductor/LICENSE.md

%pre
getent passwd conductor >/dev/null || useradd --system --user-group --no-create-home --home-dir /var/lib/conductor --shell /sbin/nologin conductor

%post
systemctl daemon-reload >/dev/null 2>&1 || :

%preun
if [ "$1" -eq 0 ]; then
    systemctl stop conductor.service >/dev/null 2>&1 || :
fi

%postun
systemctl daemon-reload >/dev/null 2>&1 || :

%files
%attr(0755,root,root) /usr/bin/conductor
%attr(0644,root,root) /usr/lib/systemd/system/conductor.service
%attr(0644,root,root) /usr/share/doc/conductor/INSTALL.md
%attr(0644,root,root) /usr/share/doc/conductor/examples/agent.json
%license /usr/share/licenses/conductor/LICENSE.md
