ARG PG_VERSION
FROM postgres:${PG_VERSION}-alpine

ARG CHECK_CODE
RUN if [ "${CHECK_CODE}" = "clang" ] ; then \
	echo 'http://dl-3.alpinelinux.org/alpine/edge/main' > /etc/apk/repositories; \
	apk --no-cache add clang-analyzer make musl-dev gcc g++ openssl-dev cmake curl-dev util-linux-dev; \
	fi

RUN if [ "${CHECK_CODE}" = "false" ] ; then \
	apk --no-cache add curl python3 gcc clang llvm g++ make musl-dev openssl-dev cmake curl-dev util-linux-dev git gdb sudo musl-dbg; \
	sed -e 's/# %wheel ALL=(ALL) NOPASSWD: ALL/%wheel ALL=(ALL) NOPASSWD: ALL/g' -i /etc/sudoers; \
	sed -e 's/^wheel:\(.*\)/wheel:\1,postgres/g' -i /etc/group; \
	fi

ENV LANG=C.UTF-8 PGDATA=/pg/data CHECK_CODE=${CHECK_CODE}

RUN mkdir -p ${PGDATA} && \
	mkdir /pg/src && \
	mkdir -p /usr/local/lib/postgresql/bitcode && \
	chown postgres:postgres ${PGDATA} && \
	chmod a+rwx /usr/local/lib/postgresql && \
	chmod a+rwx /usr/local/lib/postgresql/bitcode && \
	chmod a+rwx /usr/local/share/postgresql/extension && \
	mkdir -p /usr/local/share/doc/postgresql/contrib && \
	chmod a+rwx /usr/local/share/doc/postgresql/contrib

ADD . /pg/src
RUN rm -rf /pg/src/build
WORKDIR /pg/src
RUN chmod -R go+rwX /pg/src
USER postgres
ENTRYPOINT PGDATA=${PGDATA} CHECK_CODE=${CHECK_CODE} bash run_tests.sh
