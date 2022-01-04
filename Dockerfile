FROM golang:alpine AS build
WORKDIR /src
COPY . .
RUN cd server && go build -o /out/server .

FROM alpine
COPY --from=build /out/server /server
CMD [ "/server" ]