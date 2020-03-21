package io.cdap.cdap.app.preview;

import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
public interface ArtifactRepositoryReaderFactory {
  public ArtifactRepositoryReader create();
}
