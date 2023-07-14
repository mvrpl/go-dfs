package main

import (
	"context"
	"log"
	"net"

	"mvrpl.dev/dfsapi/fileSystemServer"

	"google.golang.org/grpc"
)

type dfsGrpc struct {
	fileSystemServer.PrincipalNoServer
}

func (m *dfsGrpc) AtribuirBlocos(ctx context.Context, request *fileSystemServer.ArquivoMetadata) (*fileSystemServer.MapeamentoLocalizacaoBloco, error) {
	return nil, nil
}

func (m *dfsGrpc) EstaAtivo(ctx context.Context, request *fileSystemServer.RelatorioBloco) (*fileSystemServer.Status, error) {
	return &fileSystemServer.Status{Success: true}, nil
}

func (m *dfsGrpc) ListaArquivos(ctx context.Context, request *fileSystemServer.ListaArquivosParam) (*fileSystemServer.ListaArquivos, error) {
	return &fileSystemServer.ListaArquivos{}, nil
}

func (m *dfsGrpc) ObterLocalizacoesBloco(ctx context.Context, request *fileSystemServer.ArquivoMetadata) (*fileSystemServer.MapeamentoLocalizacaoBloco, error) {
	return nil, nil
}

func main() {
	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("falha tcp: %v", err)
	}
	s := grpc.NewServer()
	listaArquivos := &dfsGrpc{}
	fileSystemServer.RegisterPrincipalNoServer(s, listaArquivos)
	log.Printf("servidor iniciado em %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("falha no servidor: %v", err)
	}
}
