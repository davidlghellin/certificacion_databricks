{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = { nixpkgs, ... }:
  let
    systems = [
      "x86_64-linux"
      "aarch64-linux"
      "x86_64-darwin"
      "aarch64-darwin"
    ];

    forAllSystems = nixpkgs.lib.genAttrs systems;
  in {
    devShells = forAllSystems (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };

      in {
        default = pkgs.mkShell {
          packages = with pkgs; [
            databricks-cli
            jq
            bashInteractive
            bash-completion
          ];

          shellHook = ''
            # autocompletado (tab) + completado de la CLI de databricks
            source ${pkgs.bash-completion}/share/bash-completion/bash_completion
            source <(databricks completion bash 2>/dev/null)

            # ajustes de tab: solo aplican en shell interactiva
            if [[ $- == *i* ]]; then
              bind 'set completion-ignore-case on'   # tab sin distinguir mayusculas
              bind 'set show-all-if-ambiguous on'    # menu al primer tab
              bind 'set colored-stats on'            # colorea ficheros/dirs
            fi

            # prompt: [databricks] dir (rama-git) $
            git_branch() { git branch --show-current 2>/dev/null | sed 's/.*/ (&)/'; }
            PS1='\[\e[1;32m\][databricks]\[\e[0m\] \[\e[1;34m\]\w\[\e[0m\]\[\e[1;33m\]$(git_branch)\[\e[0m\] \$ '

            echo "▸ devShell listo: databricks $(databricks --version 2>/dev/null), jq"
          '';
        };
      });
  };
}
