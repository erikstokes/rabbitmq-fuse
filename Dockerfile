FROM centos:7

RUN yum -y install gcc openssl openssl-devel fuse
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /rustup.rs \
    && chmod 700 /rustup.rs \
    && /rustup.rs -y \
    && rm /rustup.rs

WORKDIR /proj

COPY src/ /proj/src/
COPY Cargo.lock \
     Cargo.toml \
     /proj 

RUN source $HOME/.cargo/env && cargo build

